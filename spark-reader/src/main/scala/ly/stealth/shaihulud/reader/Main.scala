package ly.stealth.shaihulud.reader

import java.util.UUID

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import consumer.kafka.MessageAndMetadata
import consumer.kafka.client.KafkaReceiver
import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream

object Main extends App with Logging {
  val parser = new scopt.OptionParser[ReaderConfiguration]("spark-reader") {
    head("Spark Reader for Kafka client applications", "1.0")
    opt[String]("testId") unbounded() optional() action { (x, c) =>
      c.copy(testId = x)
    } text ("Source topic with initial set of data")
    opt[String]("source") unbounded() required() action { (x, c) =>
      c.copy(sourceTopic = x)
    } text ("Source topic with initial set of data")
    opt[String]("destination") unbounded() required() action { (x, c) =>
      c.copy(destinationTopic = x)
    } text ("Destination topic with processed set of data")
    opt[Int]("partitions") unbounded() optional() action { (x, c) =>
      c.copy(partitions = x)
    } text ("Partitions in topic")
    opt[String]("zookeeper") unbounded() required() action { (x, c) =>
      c.copy(zookeeper = x)
    } text ("Zookeeper connection host:port")
    opt[Int]("kafka.fetch.size") unbounded() optional() action { (x, c) =>
      c.copy(kafkaFetchSize = x)
    } text ("Maximum KBs to fetch from Kafka")
    checkConfig { c =>
      if (c.testId.isEmpty || c.sourceTopic.isEmpty || c.destinationTopic.isEmpty || c.zookeeper.isEmpty) {
        failure("You haven't provided all required parameters")
      } else {
        success
      }
                }
  }
  val config = parser.parse(args, ReaderConfiguration()) match {
    case Some(c) => c
    case None => sys.exit(1)
  }

  val sparkConfig = new SparkConf().setAppName("kafka_client_validator")
                                   .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  val ssc = new StreamingContext(sparkConfig, Seconds(10))
  ssc.checkpoint("reader")

  CassandraConnector(sparkConfig).withSessionDo( session => {
    session.execute("CREATE KEYSPACE IF NOT EXISTS kafka_client_validation WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    session.execute("CREATE TABLE IF NOT EXISTS kafka_client_validation.tests(test_id text PRIMARY KEY, source_topic text, destination_topic text)")
    session.execute("CREATE TABLE IF NOT EXISTS kafka_client_validation.counters(test_id text, topic text, total counter, PRIMARY KEY(test_id, topic))")
    session.execute("CREATE TABLE IF NOT EXISTS kafka_client_validation.messages(test_id text, topic text, partition int, offset int, payload text, PRIMARY KEY(test_id, topic, partition, offset))")
  })
  val test = Test(config.testId, config.sourceTopic, config.destinationTopic)
  ssc.sparkContext.parallelize(Seq(test)).saveToCassandra("kafka_client_validation", "tests")

  startStreamForTopic(test.test_id, config.sourceTopic, config)
  startStreamForTopic(test.test_id, config.destinationTopic, config)

  ssc.start()
  ssc.awaitTermination()

  def startStreamForTopic(testId: String, topic: String, config: ReaderConfiguration) {
    val stream = createKafkaStream(config.zookeeper, topic, config.partitions).repartition(config.partitions).persist(StorageLevel.MEMORY_AND_DISK_SER)
    stream.map(message => {
      Counter(testId, message.getTopic, 1L)
    }).reduce((prev, curr) => {
      Counter(testId, prev.topic, prev.total + curr.total)
    }).foreachRDD(rdd => {
      rdd.saveToCassandra("kafka_client_validation", "counters")
    })

    stream.map(message => {
      Message(testId, message.getTopic,message.getPartition.partition, message.getOffset, new String(message.getPayload))
    }).foreachRDD(rdd => {
      rdd.saveToCassandra("kafka_client_validation", "messages")
    })
  }

  private def createKafkaStream(zkConnect: String, topic: String, partitions: Int): DStream[MessageAndMetadata] = {
    val zkhosts = zkConnect.split(":")(0)
    val zkports = zkConnect.split(":")(1)
    val kafkaParams = Map("zookeeper.hosts" -> zkhosts,
                          "zookeeper.port" -> zkports,
                          "zookeeper.consumer.connection" -> zkConnect,
                          "zookeeper.broker.path" -> "/brokers",
                          "zookeeper.consumer.path" -> "/consumers",
                          "fetch.size.bytes" -> (config.kafkaFetchSize * 1024).toString,
                          "kafka.topic" -> topic,
                          "kafka.consumer.id" -> "%s-%s".format(topic, UUID.randomUUID().toString))
    val props = new java.util.Properties()
    kafkaParams foreach { case (key, value) => props.put(key, value)}
    val streams = (0 to partitions - 1).map { partitionId => ssc.receiverStream(new KafkaReceiver(StorageLevel.MEMORY_AND_DISK_SER, props, partitionId))}
    ssc.union(streams)
  }
}

case class Test(test_id: String = "", source_topic: String = "", destination_topic: String = "")
case class Counter(test_id: String = "", topic: String = "", total: Long = 0L)
case class Message(test_id: String = "", topic: String = "", partition: Int = 0, offset: Long = 0, payload: String = "")

case class ReaderConfiguration(testId: String = UUID.randomUUID().toString, sourceTopic: String = "", destinationTopic: String = "",
                                  partitions: Int = 1, zookeeper: String = "", kafkaFetchSize: Int = 8)