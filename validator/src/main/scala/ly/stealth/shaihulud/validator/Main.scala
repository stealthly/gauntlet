package ly.stealth.shaihulud.validator

import java.security.MessageDigest
import java.util.Iterator

import com.datastax.driver.core.{Cluster, Row, SocketOptions}

object Main extends App {
  val parser = new scopt.OptionParser[ValidatorConfiguration]("spark-validator") {
    head("Spark Validator for Kafka client applications", "1.0")
    opt[String]("test.id") unbounded() required() action { (x, c) =>
      c.copy(testId = x)
    } text ("Test ID")
    opt[String]("cassandra.connect") unbounded() required() action { (x, c) =>
      c.copy(cassandraConnect = x)
    } text ("Cassandra host")
    opt[String]("cassandra.user") unbounded() required() action { (x, c) =>
      c.copy(cassandraUser = x)
    } text ("Cassandra user")
    opt[String]("cassandra.password") unbounded() required() action { (x, c) =>
      c.copy(cassandraPassword = x)
    } text ("Cassandra password")
    checkConfig { c =>
      if (c.testId.isEmpty || c.cassandraConnect.isEmpty || c.cassandraUser.isEmpty || c.cassandraPassword.isEmpty) {
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

  val cluster = new Cluster.Builder()
    .addContactPoints(config.cassandraConnect)
    .withSocketOptions(new SocketOptions().setTcpNoDelay(true))
    .build()

  val session = cluster.connect("kafka_client_validation")

  val tests = session.execute("SELECT * FROM kafka_client_validation.tests WHERE test_id='%s'".format(config.testId))
  val test = tests.one()
  if (test != null) {
    val testId = test.getString("test_id")
    val sourceTopic = test.getString("source_topic")
    val destinationTopic = test.getString("destination_topic")

    val countersQuery = "SELECT * FROM kafka_client_validation.counters WHERE test_id='%s' AND topic='%s'"
    val sourceCounter = session.execute(countersQuery.format(testId, sourceTopic))
    val destinationCounter = session.execute(countersQuery.format(testId, destinationTopic))

    println("***** TEST RESULTS *****")
    var sameAmount = false
    val totalInSource = sourceCounter.one().getLong("total")
    val totalInDestination = destinationCounter.one().getLong("total")
    if (totalInSource == totalInDestination) {
      sameAmount = true
    }
    println(" - Destination topic contains the same amount of messages as Source topic(%d out of %d): %B".format(totalInSource,
                                                                                                                 totalInDestination,
                                                                                                                 sameAmount))
    val messagesQuery = "SELECT * FROM kafka_client_validation.messages WHERE test_id='%s' AND topic='%s'"
    val sourceMessages = session.execute(messagesQuery.format(testId, sourceTopic))
    val destinationMessages = session.execute(messagesQuery.format(testId, destinationTopic))
    val si = sourceMessages.iterator()
    val di = destinationMessages.iterator()

    val portionSize = 1000
    var isOrderPreserved = true
    while ((si.hasNext || di.hasNext) && isOrderPreserved) {
      val sourceHash = this.calculateMD5ForSlice(si, portionSize)
      val destinationHash = this.calculateMD5ForSlice(di, portionSize)
      if (sourceHash != destinationHash) {
        isOrderPreserved = false
      }
    }
    println(" - Destination topic preserves ordering of Source topic: %B".format(isOrderPreserved))
  } else {
    System.err.println("There is no such test '%s'".format(config.testId))
  }

  cluster.close()

  def calculateMD5ForSlice(it: Iterator[Row], portionSize: Int): String = {
    val sb = new StringBuilder
    var left = portionSize
    while (it.hasNext && left > 0) {
      sb.append(it.next.getString("payload"))
      left = left - 1
    }

    new String(MessageDigest.getInstance("MD5").digest(sb.toString().getBytes("UTF-8")))
  }
}

case class ValidatorConfiguration(testId: String = "", cassandraConnect: String = "", cassandraUser: String = "", cassandraPassword: String = "")