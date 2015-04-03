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

package ly.stealth.shaihulud.datasetproducer

import java.io.{EOFException, File, FileInputStream, IOException}
import java.net._
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.util.Properties

import kafka.producer.{KeyedMessage, Producer}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.KafkaException

import scala.collection._
import scala.io.Source
import scala.util.{Failure, Success, Try}

object Main {
  def main(args: Array[String]) {
    val config = parseConfig(args)
    val producer = DatasetProducer(config)

    sys.addShutdownHook {
      producer.stop()
    }

    producer.start()
  }

  def parseConfig(args: Array[String]): DatasetProducerConfig = {
    val parser = new scopt.OptionParser[DatasetProducerConfig]("dataset-producer") {
      head("Dataset Producer")
      opt[String]('f', "filename").required().text("Name of dataset file to produce from.").action {
        (value, config) =>
          config.copy(filename = value)
      }

      opt[String]('k', "kafka").optional().text("Kafka broker address.").action {
        (value, config) =>
          config.copy(kafka = Some(value))
      }

      opt[File]('p', "producer.config").optional().text("Kafka producer properties file.").action {
        (value, config) =>
          config.copy(producerProperties = Some(value))
      }

      opt[String]('t', "topic").optional().text("Kafka destination topic.").action {
        (value, config) =>
          config.copy(topic = Some(value))
      }

      opt[String]('s', "syslog").optional().text("Syslog server address. Format: protocol://host:port (tcp://0.0.0.0:5140 or udp://0.0.0.0:5141 for example)").action {
        (value, config) =>
          config.copy(syslog = Some(value))
      }

      opt[Boolean]('l', "loop").text("Flag to loop through file forever.").action {
        (value, config) =>
          config.copy(loop = value)
      }

      opt[Unit]("avro").text("Avro schema produce mode").optional().action {
        (value, config) =>
          config.copy(avro = true)
      }

      opt[File]("schema.path").text("Path to Avro schemas.").optional().action {
        (value, config) =>
          config.copy(schemaPath = Some(value))
      }

      opt[String]("schema.registry.url").text("Avro schema repository URL.").optional().action {
        (value, config) =>
          config.copy(schemaRepoUrl = value)
      }
    }

    parser.parse(args, DatasetProducerConfig()) match {
      case Some(config) =>
        if ((config.kafka.isDefined && config.syslog.isDefined) || (config.kafka.isEmpty && config.syslog.isEmpty)) {
          println("Exactly one --kafka or --syslog flag is required.")
          sys.exit(1)
        }
        if (config.kafka.isDefined && !config.avro && (config.producerProperties.isEmpty || config.topic.isEmpty)) {
          println("--producer.config and --topic flags are required when using --kafka flag.")
          sys.exit(1)
        }
        if (config.avro && (config.schemaPath.isEmpty || config.schemaRepoUrl.isEmpty)) {
          println("--schema.path and --schema.registry.url flags are required when using --avro flag.")
          sys.exit(1)
        }
        config
      case None => sys.exit(1)
    }
  }
}

case class DatasetProducerConfig(filename: String = "", kafka: Option[String] = None, producerProperties: Option[File] = None,
                                 topic: Option[String] = None, syslog: Option[String] = None, loop: Boolean = false,
                                 avro: Boolean = false, schemaPath: Option[File] = None, schemaRepoUrl: String = "")

case class DatasetProducer(config: DatasetProducerConfig) {
  var stopRequested = false
  val UDP = "udp"
  val TCP = "tcp"

  def start() {
    if (this.config.kafka.isDefined) {
      if (config.avro) {
        this.sendAvroKafka()
      } else {
        this.sendPlainKafka()
      }
    } else this.sendSyslog()
  }

  def sendPlainKafka() {
    val props = new Properties()
    props.load(new FileInputStream(this.config.producerProperties.get))
    val producer = new Producer[Any, Any](new kafka.producer.ProducerConfig(props))
    do {
      Source.fromFile(this.config.filename, "UTF-8").getLines().foreach { line =>
        if (this.stopRequested) {
          producer.close()
          return
        }
        val sendData = line.getBytes("UTF-8")
        producer.send(new KeyedMessage(this.config.topic.get, sendData))
      }
      Thread.sleep(1) // this helps avoid "Too many open files" exception on small files
    } while (this.config.loop)
  }

  def sendAvroKafka() {
    //Producer initialization
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafka.get)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
              classOf[io.confluent.kafka.serializers.KafkaAvroSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
              classOf[io.confluent.kafka.serializers.KafkaAvroSerializer])
    props.put("schema.registry.url", config.schemaRepoUrl)
    val producer = new KafkaProducer[Any, Any](props)

    val schemas = resolveAvroSchemas.iterator
    var datumReader = new GenericDatumReader[GenericRecord](schemas.next())
    var eof = false
    do {
      eof = false
      if (this.stopRequested) {
        producer.close()
        return
      }
      val in = Files.newInputStream(Paths.get(this.config.filename))
      val decoder = DecoderFactory.get().directBinaryDecoder(in, null)
      do {
        var data: GenericRecord = null
        //If user provided more than one schema, then we'll pick appropriate one
        while (data == null && !eof) {
          try {
            data = datumReader.read(data, decoder)
            val record = new ProducerRecord[Any, Any](config.topic.get, data)
            producer.send(record).get()
          } catch {
            case e: EOFException => {
              eof = true
            }
            case e: IOException => {
              if (!schemas.hasNext) {
                println("Failed to decode entries with provided schemas.")
                sys.exit(1)
              }
              datumReader = new GenericDatumReader[GenericRecord](schemas.next())
            }
            case e: KafkaException => {
              println("Failed to send message to Kafka.")
              sys.exit(1)
            }
          }
        }

        Thread.sleep(1) // this helps avoid "Too many open files" exception on small files
      } while (!eof)
    } while (this.config.loop)
  }

  def sendSyslog() {
    val (protocol, host, port) = this.resolveAddress(this.config.syslog.get)
    protocol match {
      case TCP => this.sendTCP(host, port)
      case UDP => this.sendUDP(host, port)
      case other => throw new IllegalArgumentException(s"Invalid protocol name: $other")
    }
  }

  def sendTCP(ip: InetAddress, port: Int) {
    val socket = new Socket(ip, port)
    try {
      val socketOutputStream = socket.getOutputStream
      try {
        sender(data => socketOutputStream.write(data, 0, data.length))
      } finally {
        socketOutputStream.close()
      }
    } finally {
      socket.close()
    }
  }

  def sendUDP(ip: InetAddress, port: Int) {
    val socket = new DatagramSocket()
    try {
      sender(data => socket.send(new DatagramPacket(data, data.length, ip, port)))
    } finally {
      socket.close()
    }
  }

  def stop() = this.stopRequested = true

  private def resolveAddress(address: String): (String, InetAddress, Int) = {
    val Array(protocol, address) = this.config.syslog.get.split("://")
    val Array(host, port) = address.split(":")
    val ip = Try(InetAddress.getByName(host)) match {
      case Success(addr) => addr
      case Failure(_) =>
        Try(InetAddress.getByName(host)) match {
          case Success(addr) => addr
          case Failure(e) => throw e
        }
    }
    (protocol, ip, port.toInt)
  }

  private def resolveAvroSchemas: List[Schema] = {
    if (!config.schemaPath.get.exists) {
      println("Schema path '%s' does not exist".format(config.schemaPath))
      sys.exit(1)
    }
    val parser = new Schema.Parser()
    if (config.schemaPath.get.isDirectory) {
      val schemas = new mutable.MutableList[Schema]
      Files.walkFileTree(config.schemaPath.get.toPath, new SimpleFileVisitor[Path]() {
        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
          schemas += parser.parse(file.toFile)
          FileVisitResult.CONTINUE
        }
      })
      schemas.toList
    } else {
      List(parser.parse(config.schemaPath.get))
    }
  }

  private def sender(send: Array[Byte] => Any) {
    do {
      Source.fromFile(this.config.filename).getLines().foreach { line =>
        if (this.stopRequested) return
        val data = (line + "\n").getBytes("UTF-8")
        send(data)
      }
      Thread.sleep(1) // this helps avoid "Too many open files" exception on small files
    } while (this.config.loop)
  }
}