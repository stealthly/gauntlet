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

import _root_.kafka.serializer.StringDecoder
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
import org.apache.spark.rdd._
import org.apache.spark.streaming.dstream.{ReceiverInputDStream, DStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.storage.StorageLevel

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
    checkConfig { c =>
      if (c.sourceTopic.isEmpty || c.destinationTopic.isEmpty || c.zookeeper.isEmpty) failure("You haven't provided all required parameters") else success
                }
  }
  val config = parser.parse(args, ValidatorConfiguration()) match {
    case Some(c) => c
    case None => sys.exit(1)
  }

  val sparkConfig = new SparkConf().setMaster("local[4]").setAppName("kafka-client-validator")
  val ssc = new StreamingContext(sparkConfig, Seconds(1))
  val kafkaSourceStreams = createKafkaStream(config.zookeeper, "kafka-client-validator-source", config.sourceTopic, config.partitions)
  val kafkaDestinationStreams = createKafkaStream(config.zookeeper, "kafka-client-validator-destination", config.destinationTopic, config.partitions)

  val sourceSet = new scala.collection.mutable.HashSet[String]()
  val destinationSet = new scala.collection.mutable.HashSet[String]()
  kafkaSourceStreams.cogroup(kafkaDestinationStreams).foreachRDD(rdd => {
    rdd.collect().foreach(entry => {
      entry._2._1.foreach(sourceSet.+=(_))
      entry._2._2.foreach(destinationSet.+=(_))
    })
  })

  ssc.start()
  ssc.awaitTermination(60000)

  val success = sourceSet.forall(destinationSet.contains(_))

  println("******** TEST RESULTS ********")
  if (success) {
    println("TEST PASSED")
  } else {
    System.err.println("TEST FAILED")
  }
  println("All source topic entries were reflected into destination topic: %B".format(success))
  println("Amount of entries in source topic: %d".format(sourceSet.size))
  println("Amount of entries in destination topic: %d".format(destinationSet.size))
  println("Amount of duplicates in destination topic: %d".format(Math.max(destinationSet.size - sourceSet.size, 0)))

  ssc.stop()

  def createKafkaStream(zkConnect: String, consumerGroup: String, topic: String, partitions: Int): ReceiverInputDStream[(String, String)] = {
    val kafkaParams = Map("zookeeper.connect" -> zkConnect,
                          "group.id" -> consumerGroup,
                          "zookeeper.connection.timeout.ms" -> "1000",
                          "auto.offset.reset" -> "smallest")

    val topics: Map[String, Int] = Map(topic -> partitions)
    KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics, StorageLevel.MEMORY_ONLY)
  }
}

case class ValidatorConfiguration(sourceTopic: String = "", destinationTopic: String = "",
                                  partitions: Int = 1, zookeeper: String = "")