/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 package ly.stealth.shaihulud.validator

import java.sql.DriverManager
import java.util.UUID

import consumer.kafka.MessageAndMetadata
import consumer.kafka.client.KafkaReceiver
import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable

object Main extends App with Logging {
  val parser = new scopt.OptionParser[ValidatorConfiguration]("spark-validator") {
    head("Spark Validator for Kafka client applications", "1.0")
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
    opt[String]("mysql.connect") unbounded() required() action { (x, c) =>
      c.copy(mysqlConnect = x)
    } text ("MySQL connection host:port")
    opt[String]("mysql.user") unbounded() required() action { (x, c) =>
      c.copy(mysqlUser = x)
    } text ("MySQL user")
    opt[String]("mysql.password") unbounded() required() action { (x, c) =>
      c.copy(mysqlPassword = x)
    } text ("MySQL password")
    opt[String]("mysql.db") unbounded() required() action { (x, c) =>
      c.copy(mysqlDbName = x)
    } text ("MySQL database name")
    opt[Int]("kafka.fetch.size") unbounded() optional() action { (x, c) =>
      c.copy(kafkaFetchSize = x)
    } text ("Maximum KBs to fetch from Kafka")
    checkConfig { c =>
      if (c.sourceTopic.isEmpty || c.destinationTopic.isEmpty || c.zookeeper.isEmpty
          || c.mysqlConnect.isEmpty || c.mysqlDbName.isEmpty || c.mysqlUser.isEmpty) {
        failure("You haven't provided all required parameters")
      } else {
        success
      }
                }
  }

  val config = parser.parse(args, ValidatorConfiguration()) match {
    case Some(c) => c
    case None => sys.exit(1)
  }

  val sparkConfig = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  val ssc = new StreamingContext(sparkConfig, Seconds(10))
  ssc.checkpoint("validator")
  val testId = UUID.randomUUID().toString

  startStreamForTopic(config.sourceTopic, config)
  startStreamForTopic(config.destinationTopic, config)

  val conn = DriverManager.getConnection("jdbc:mysql://%s/%s?user=%s&password=%s".format(config.mysqlConnect,
                                                                                         config.mysqlDbName,
                                                                                         config.mysqlUser,
                                                                                         config.mysqlPassword))

  val statement = conn.createStatement
  statement.execute("CREATE TABLE IF NOT EXISTS kafka_spark_validator(test_id varchar(255) not null, topic varchar(255) not null, total int not null, order_hash varchar(255),  primary key(test_id, topic))")
  statement.execute("INSERT INTO kafka_spark_validator VALUES('%s', '%s', %d, '%s')".format(testId, config.sourceTopic, 0, ""))
  statement.execute("INSERT INTO kafka_spark_validator VALUES('%s', '%s', %d, '%s')".format(testId, config.destinationTopic, 0, ""))

  ssc.start()
  ssc.awaitTermination()

  def startStreamForTopic(topic: String, config: ValidatorConfiguration) {
    val stream = createKafkaStream(config.zookeeper, topic, config.partitions).repartition(config.partitions).map(rdd => {
      (rdd.getPartition.partition, new String(rdd.getPayload))
    }).persist(StorageLevel.MEMORY_AND_DISK_SER)

    stream.count().foreachRDD(rdd => {
      if (rdd.count() > 0) {
        val conn = DriverManager.getConnection("jdbc:mysql://%s/%s?user=%s&password=%s".format(config.mysqlConnect,
                                                                                               config.mysqlDbName,
                                                                                               config.mysqlUser,
                                                                                               config.mysqlPassword))
        val totalUpdate = conn.prepareStatement("UPDATE kafka_spark_validator SET total = total + ?  WHERE test_id = ? AND topic = ?")

        totalUpdate.setLong(1, rdd.first())
        totalUpdate.setString(2, testId)
        totalUpdate.setString(3, topic)
        totalUpdate.execute()
        totalUpdate.close()
      }
    })

    val initial: mutable.Map[Int, mutable.MutableList[String]] = mutable.HashMap[Int, mutable.MutableList[String]]()
    val messageAccumulator = ssc.sparkContext.accumulator(initial, topic)(PartitionMessageAccumulatorParam)
    stream.foreachRDD(rdd => {
      val messageMap = new mutable.HashMap[Int, mutable.MutableList[String]]
      for ((partition, message) <- rdd.collect()) {
        if (!messageMap.contains(partition)) {
          messageMap.put(partition, new mutable.MutableList[String]())
        }
        messageMap(partition) += message
      }
      messageAccumulator.add(messageMap)

      val hash = DigestUtils.md5Hex(messageAccumulator.value.map(_._2.mkString("")).mkString(""))
      val conn = DriverManager.getConnection("jdbc:mysql://%s/%s?user=%s&password=%s".format(config.mysqlConnect,
                                                                                             config.mysqlDbName,
                                                                                             config.mysqlUser,
                                                                                             config.mysqlPassword))
      val orderUpdate = conn.prepareStatement("UPDATE kafka_spark_validator SET order_hash = ? WHERE test_id = ? AND topic = ?")
      orderUpdate.setString(1, hash)
      orderUpdate.setString(2, testId)
      orderUpdate.setString(3, topic)
      orderUpdate.execute()
      orderUpdate.close()
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

case class ValidatorConfiguration(sourceTopic: String = "", destinationTopic: String = "",
                                  partitions: Int = 1, zookeeper: String = "", mysqlUser: String = "",
                                  mysqlPassword: String = "", mysqlDbName: String = "", mysqlConnect: String = "",
                                  kafkaFetchSize: Int = 8)

object PartitionMessageAccumulatorParam extends AccumulatorParam[mutable.Map[Int, mutable.MutableList[String]]] {
  def addInPlace(r1: mutable.Map[Int, mutable.MutableList[String]], r2: mutable.Map[Int, mutable.MutableList[String]]): mutable.Map[Int, mutable.MutableList[String]] = {
    for ((partition, messages) <- r2) {
      if (!r1.contains(partition)) {
        r1.put(partition, new mutable.MutableList[String]())
      }
      for (message <- messages) {
        r1(partition) += message
      }
    }

    r1
  }

  def zero(initialValue: mutable.Map[Int, mutable.MutableList[String]]): mutable.Map[Int, mutable.MutableList[String]] = {
    new mutable.HashMap[Int, mutable.MutableList[String]]
  }
}