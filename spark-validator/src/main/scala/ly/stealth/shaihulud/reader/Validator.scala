package ly.stealth.shaihulud.reader

import java.net.InetAddress
import java.security.MessageDigest
import scala.collection.JavaConversions._

import com.datastax.driver.core.{Row, SocketOptions, Cluster}

class Validator(config: ReaderConfiguration, hosts: Set[InetAddress]) {

  def validate() {
    val cluster = new Cluster.Builder()
      .addContactPoints(hosts)
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
  }

  private def calculateMD5ForSlice(it: Iterator[Row], portionSize: Int): String = {
    val sb = new StringBuilder
    var left = portionSize
    while (it.hasNext && left > 0) {
      sb.append(it.next.getString("payload"))
      left = left - 1
    }

    new String(MessageDigest.getInstance("MD5").digest(sb.toString().getBytes("UTF-8")))
  }
}
