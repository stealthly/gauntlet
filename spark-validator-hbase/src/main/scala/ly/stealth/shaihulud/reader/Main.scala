package ly.stealth.shaihulud.reader

import java.util.{Properties, UUID}

import it.nerdammer.spark.hbase._
import consumer.kafka.MessageAndMetadata
import consumer.kafka.client.KafkaReceiver
import it.nerdammer.spark.hbase.conversion.FieldWriter
import kafka.producer.{KeyedMessage, ProducerConfig, Producer}
import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.hadoop.hbase.util.Bytes

object Main extends App with Logging {
  val parser = new scopt.OptionParser[ReaderConfiguration]("spark-reader") {
    head("Spark Reader for Kafka client applications", "1.0")
    opt[String]("source") unbounded() required() action { (value, config) =>
      config.copy(sourceTopic = value)
    } text ("Source topic with initial set of data")
    opt[String]("destination") unbounded() required() action { (value, config) =>
      config.copy(destinationTopic = value)
    } text ("Destination topic with processed set of data")
    opt[Int]("partitions") unbounded() optional() action { (value, config) =>
      config.copy(partitions = value)
    } text ("Partitions in topic")
    opt[String]("zookeeper") unbounded() required() action { (value, config) =>
      config.copy(zookeeper = value)
    } text ("Zookeeper connection host:port")
    opt[String]("broker.list") unbounded() required() action { (value, config) =>
      config.copy(brokerList = value)
    } text ("Comma separated string of host:port")
    opt[Int]("kafka.fetch.size") unbounded() optional() action { (value, config) =>
      config.copy(kafkaFetchSize = value)
    } text ("Maximum KBs to fetch from Kafka")
    checkConfig { c =>
      if (c.testId.isEmpty || c.sourceTopic.isEmpty || c.destinationTopic.isEmpty || c.zookeeper.isEmpty || c.brokerList.isEmpty) {
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
    .set("spark.hbase.host", config.zookeeper)
    .setMaster("local[8]")
  val ssc = new StreamingContext(sparkConfig, Seconds(10))
  ssc.checkpoint("spark-validator-hbase")

  implicit def TestFieldWriter: FieldWriter[Test] = new FieldWriter[Test] {
    override def map(data: Test): HBaseData =
      Seq(
        Some(Bytes.toBytes(data.test_id)),
        Some(Bytes.toBytes(data.source_topic)),
        Some(Bytes.toBytes(data.destination_topic))
      )

    override def columns = Seq("source_topic", "destination_topic")
  }

  implicit def MessageFieldWriter: FieldWriter[Message] = new FieldWriter[Message] {
    override def map(data: Message): HBaseData =
      Seq(
        Some(Bytes.toBytes(data.id)),
        Some(Bytes.toBytes(data.test_id)),
        Some(Bytes.toBytes(data.topic)),
        Some(Bytes.toBytes(data.partition)),
        Some(Bytes.toBytes(data.offset)),
        Some(Bytes.toBytes(data.payload))
      )

    override def columns = Seq("test_id", "topic", "partition", "offset", "payload")
  }

  val poisonPill = UUID.randomUUID().toString.getBytes("UTF8")
  markStreamEnd(poisonPill)

  val test = Test(config.testId, config.sourceTopic, config.destinationTopic)
  ssc.sparkContext.parallelize(Seq(test))
    .toHBaseTable("tests")
    .inColumnFamily("kafka_client_validation")
    .save()
  val acc = ssc.sparkContext.accumulator[Int](0, "finishedPartitions")

  val validator = new Validator(config)

  startStreamForTopic(test.test_id, config.sourceTopic, config, poisonPill, validator)
  startStreamForTopic(test.test_id, config.destinationTopic, config, poisonPill, validator)

  ssc.start()
  ssc.awaitTermination()

  def markStreamEnd(poisonPill: Array[Byte]) {
    //Producing poison pill message to each partition of specified source and destination topics
    //in order to determine end of the stream
    val props = new Properties()
    props.put("metadata.broker.list", config.brokerList)
    props.put("producer.type", "sync")
    val producerConfig = new ProducerConfig(props)
    val producer = new Producer[Array[Byte], Array[Byte]](producerConfig)
    (0 until config.partitions).foreach(partition => {
      producer.send(new KeyedMessage(config.sourceTopic, null, partition, poisonPill))
      producer.send(new KeyedMessage(config.destinationTopic, null, partition, poisonPill))
      logInfo("Marked stream end for partition %d with sequence %s".format(partition, poisonPill))
    })
  }

  def startStreamForTopic(testId: String, topic: String, config: ReaderConfiguration, poisonPill: Array[Byte], validator: Validator) {
    val stream = createKafkaStream(config.zookeeper, topic, config.partitions).repartition(config.partitions).persist(StorageLevel.MEMORY_AND_DISK_SER)
    stream.map(message => {
      Message(testId, message.getTopic, message.getPartition.partition, message.getOffset, new String(message.getPayload))
    }).foreachRDD(rdd => {
      val filtered = rdd.filter(message => !java.util.Arrays.equals(message.payload.getBytes("UTF8"), poisonPill))
      filtered.toHBaseTable("messages")
        .inColumnFamily("kafka_client_validation")
        .save()
      if (rdd.count() > filtered.count()) {
        logInfo("End of the stream reached")
        acc.add((rdd.count() - filtered.count()).toInt)
        if (acc.value == config.partitions * 2) {
          validator.validate()
          ssc.stop(true)
        }
      }
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

case class Message(test_id: String = "", topic: String = "", partition: Int = 0, offset: Long = 0, payload: String = "") {
  val id = test_id + topic + partition + offset
}

case class ReaderConfiguration(testId: String = UUID.randomUUID().toString, sourceTopic: String = "", destinationTopic: String = "",
                               partitions: Int = 1, zookeeper: String = "", brokerList: String = "", kafkaFetchSize: Int = 8)