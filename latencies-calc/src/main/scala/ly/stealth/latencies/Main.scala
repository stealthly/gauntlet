package ly.stealth.latencies

import _root_.kafka.serializer.DefaultDecoder
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import io.confluent.kafka.serializers.KafkaAvroDecoder
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka._

object Main extends App {
  val parser = new scopt.OptionParser[AppConfig]("spark-analysis") {
    head("Latencies calculation job", "1.0")
    opt[String]("topic") unbounded() required() action { (value, config) =>
      config.copy(topic = value)
    } text ("Topic to read data from")
    opt[String]("broker.list") unbounded() required() action { (value, config) =>
      config.copy(brokerList = value)
    } text ("Comma separated string of host:port")
    opt[String]("schema.registry.url") unbounded() required() action { (value, config) =>
      config.copy(schemaRegistryUrl = value)
    } text ("Schema registry URL")
    checkConfig { c =>
      if (c.topic.isEmpty || c.brokerList.isEmpty) {
        failure("You haven't provided all required parameters")
      } else {
        success
      }
    }
  }
  val appConfig = parser.parse(args, AppConfig()) match {
    case Some(c) => c
    case None => sys.exit(1)
  }

  val sparkConfig = new SparkConf().setAppName("spark-analysis").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  val ssc = new StreamingContext(sparkConfig, Seconds(1))
  ssc.checkpoint("spark-analysis")

  val cassandraConnector = CassandraConnector(sparkConfig)
  cassandraConnector.withSessionDo(session => {
    session.execute("CREATE KEYSPACE IF NOT EXISTS spark_analysis WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    session.execute("CREATE TABLE IF NOT EXISTS spark_analysis.events(topic text, partition text, consumerid text, eventname text, second int, operation text, value int, cnt int, PRIMARY KEY(topic, second, partition, consumerid, eventname, operation))")
  })

  val consumerConfig = Map("metadata.broker.list" -> appConfig.brokerList,
    "auto.offset.reset" -> "smallest",
    "schema.registry.url" -> appConfig.schemaRegistryUrl)

  start(ssc, consumerConfig, appConfig.topic)

  ssc.start()
  ssc.awaitTermination()

  def start(ssc: StreamingContext, consumerConfig: Map[String, String], topic: String) = {
    val stream = KafkaUtils.createDirectStream[Array[Byte], AnyRef, DefaultDecoder, KafkaAvroDecoder](ssc, consumerConfig, Set(topic))
    stream.persist()
    calculateAverages(stream, "second", 10)
    calculateAverages(stream, "second", 30)
    calculateAverages(stream, "minute", 1)
    calculateAverages(stream, "minute", 5)
    calculateAverages(stream, "minute", 10)
    calculateAverages(stream, "minute", 15)
  }

  def calculateAverages(stream: InputDStream[(Array[Byte], AnyRef)], durationUnit: String, durationValue: Long) = {
    stream.window(windowDuration(durationUnit, durationValue)).map(value => {
      val record = value.asInstanceOf[GenericRecord]
      import scala.collection.JavaConversions._
      val tags = record.get("tag").asInstanceOf[java.util.Map[java.lang.String, java.lang.String]]
      val timings = record.get("timings").asInstanceOf[GenericData.Array[Record]]
      timings.combinations(2).map(entry => {
        (tags.get("topic"),
          tags.get("partition"),
          tags.get("consumerId"),
          entry.head.get("key").asInstanceOf[String] + "-" + entry.last.get("key").asInstanceOf[String],
          entry.last.get("value").asInstanceOf[Long] - entry.head.get("value").asInstanceOf[Long])
      }).toList
    }).reduce((acc, value) => {
      acc ++ value
    }).flatMap(entry => {
      val second = System.currentTimeMillis()/1000
      entry.groupBy(entry => (entry._1, entry._2, entry._3, entry._4)).map { case (key, values) => {
        val timings = values.map(_._5)
        (key._1, key._2, key._3, key._4, second, "avg%d%s".format(durationValue, durationUnit), timings.sum / timings.size, timings.size)
      }
      }
    }).foreachRDD(rdd => {
      rdd.saveToCassandra("spark_analysis", "events", SomeColumns("topic", "partition", "consumerid", "eventname", "second", "operation", "value", "cnt"))
    })
  }
  
  def windowDuration(unit: String, durationValue: Long): Duration = unit match {
    case "second" => Seconds(durationValue)
    case "minute" => Minutes(durationValue)
  }
}

case class AppConfig(topic: String = "", brokerList: String = "", schemaRegistryUrl: String = "")
