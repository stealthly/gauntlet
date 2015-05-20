package ly.stealth.latencies

import java.util.Properties

import _root_.kafka.serializer.DefaultDecoder
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}
import org.apache.kafka.clients.producer.ProducerConfig._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
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
  val producerConfig = new Properties()
  producerConfig.put(BOOTSTRAP_SERVERS_CONFIG, appConfig.brokerList)
  producerConfig.put(KEY_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
  producerConfig.put(VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
  producerConfig.put("schema.registry.url", appConfig.schemaRegistryUrl)

  start(ssc, consumerConfig, producerConfig, appConfig.topic)

  ssc.start()
  ssc.awaitTermination()

  def start(ssc: StreamingContext, consumerConfig: Map[String, String], producerConfig: Properties, topic: String) = {
    val stream = KafkaUtils.createDirectStream[Array[Byte], SchemaAndData, DefaultDecoder, AvroDecoder](ssc, consumerConfig, Set(topic)).persist()
    calculateAverages(stream, "second", 10, topic, producerConfig)
    calculateAverages(stream, "second", 30, topic, producerConfig)
    calculateAverages(stream, "minute", 1, topic, producerConfig)
    calculateAverages(stream, "minute", 5, topic, producerConfig)
    calculateAverages(stream, "minute", 10, topic, producerConfig)
    calculateAverages(stream, "minute", 15, topic, producerConfig)
  }

  def calculateAverages(stream: DStream[(Array[Byte], SchemaAndData)], durationUnit: String, durationValue: Long, topic: String, producerConfig: Properties) = {
    val latencyStream = stream.window(windowDuration(durationUnit, durationValue)).map(value => {
      val record = value._2.deserialize().asInstanceOf[GenericRecord]
      import scala.collection.JavaConversions._
      val tags = record.get("tag").asInstanceOf[java.util.Map[Utf8, Utf8]]
      val timings = record.get("timings").asInstanceOf[GenericData.Array[Record]]
      timings.combinations(2).map(entry => {
        (tags.get(new Utf8("topic")).toString,
          tags.get(new Utf8("partition")).toString,
          tags.get(new Utf8("consumerId")).toString,
          entry.head.get("key").asInstanceOf[Utf8].toString + "-" + entry.last.get("key").asInstanceOf[Utf8].toString,
          entry.last.get("value").asInstanceOf[Long] - entry.head.get("value").asInstanceOf[Long])
      }).toList
    }).reduce((acc, value) => {
      acc ++ value
    }).flatMap(entry => {
      val second = System.currentTimeMillis()/1000
      entry.groupBy(entry => (entry._1, entry._2, entry._3, entry._4)).map { case (key, values) => {
        val timings = values.map(_._5)
        Event(key._1, key._2, key._3, key._4, second, "avg%d%s".format(durationValue, durationUnit), timings.sum / timings.size, timings.size)
      }
      }
    }).persist()

    val schema = "{\"type\":\"record\",\"name\":\"event\",\"fields\":[{\"name\":\"topic\",\"type\":\"string\"},{\"name\":\"partition\",\"type\":\"string\"},{\"name\":\"consumerid\",\"type\":\"string\"},{\"name\":\"eventname\",\"type\":\"string\"},{\"name\":\"second\",\"type\":\"long\"},{\"name\":\"operation\",\"type\":\"string\"},{\"name\":\"value\",\"type\":\"long\"},{\"name\":\"cnt\",\"type\":\"long\"}]}"
    val eventSchema = new Schema.Parser().parse(schema)
    latencyStream.foreachRDD(rdd => {
      rdd.foreachPartition(latencies => {
        val producer = new KafkaProducer[Any, AnyRef](producerConfig)
        try {
          for (latency <- latencies) {
              val latencyRecord = new GenericData.Record(eventSchema)
              latencyRecord.put("topic", latency.topic)
              latencyRecord.put("partition", latency.partition)
              latencyRecord.put("consumerid", latency.consumerid)
              latencyRecord.put("eventname", latency.eventname)
              latencyRecord.put("second", latency.second)
              latencyRecord.put("operation", latency.operation)
              latencyRecord.put("value", latency.value)
              latencyRecord.put("cnt", latency.cnt)
              val record = new ProducerRecord[Any, AnyRef]("%s-latencies".format(topic), latencyRecord)
              producer.send(record)
          }
        } finally {
          producer.close()
        }
      })
    })

    latencyStream.foreachRDD(rdd => {
      rdd.saveToCassandra("spark_analysis", "events")
    })
  }

  def windowDuration(unit: String, durationValue: Long): Duration = unit match {
    case "second" => Seconds(durationValue)
    case "minute" => Minutes(durationValue)
  }
}

case class Event(topic: String, partition: String, consumerid: String, eventname: String, second: Long, operation: String, value: Long, cnt: Long)
case class AppConfig(topic: String = "", brokerList: String = "", schemaRegistryUrl: String = "")
