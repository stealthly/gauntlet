package ly.stealth.shaihulud.reader

import java.io.{ByteArrayOutputStream, FileOutputStream, PrintStream}
import java.security.MessageDigest

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Get, HTable, Result, Scan}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.util.Bytes

class Validator(config: ReaderConfiguration) extends Serializable {
  val columnFamily = "kafka_client_validation"

  def validate() {
    var out: PrintStream = null
    try {
      out = new PrintStream(new FileOutputStream("gauntlet-test-%s.txt".format(config.testId)), true)
      System.setOut(out)

      val hbaseConfig = HBaseConfiguration.create()
      hbaseConfig.set("hbase.zookeeper.quorum", config.zookeeper)

      val testsTable = new HTable(hbaseConfig, "tests")
      val result = testsTable.get(new Get(Bytes.toBytes(config.testId)))
      if (result.isEmpty) {
        System.err.println("There is no such test '%s'".format(config.testId))
        return
      }

      val source = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes("source_topic"))
      val destination = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes("destination_topic"))

      val messagesTable = new HTable(hbaseConfig, "messages")
      val sourceCount = getCount(messagesTable, hbaseConfig, columnFamily, Bytes.toBytes("source_topic"), source)
      val destinationCount = getCount(messagesTable, hbaseConfig, columnFamily, Bytes.toBytes("destination_topic"), destination)
      var sameAmount = false
      if (sourceCount == destinationCount) {
        sameAmount = true
      }
      println("***** TEST RESULTS *****")
      println(" - Destination topic contains the same amount of messages as Source topic(%d out of %d): %B".format(sourceCount, destinationCount, sameAmount))

      val testFilter = new SingleColumnValueFilter(Bytes.toBytes(columnFamily), Bytes.toBytes("test_id"), CompareOp.EQUAL, Bytes.toBytes(config.testId))
      val sourceFilter = new SingleColumnValueFilter(Bytes.toBytes(columnFamily), Bytes.toBytes("topic"), CompareOp.EQUAL, source)
      val destinationFilter = new SingleColumnValueFilter(Bytes.toBytes(columnFamily), Bytes.toBytes("topic"), CompareOp.EQUAL, destination)

      val sourceScan = new Scan
      sourceScan.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("test_id"))
      sourceScan.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("topic"))
      sourceScan.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("payload"))
      sourceScan.setFilter(new FilterList(testFilter, sourceFilter))
      val sourceScanner = messagesTable.getScanner(sourceScan)
      val si = sourceScanner.iterator()

      val destinationScan = new Scan
      destinationScan.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("test_id"))
      destinationScan.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("topic"))
      destinationScan.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("payload"))
      destinationScan.setFilter(new FilterList(testFilter, destinationFilter))
      val destinationScanner = messagesTable.getScanner(destinationScan)
      val di = destinationScanner.iterator()

      var isOrderPreserved = true
      val portionSize = 1000
      try {
        while ((si.hasNext || di.hasNext) && isOrderPreserved) {
          val sourceHash = this.calculateMD5ForSlice(si, portionSize)
          val destinationHash = this.calculateMD5ForSlice(di, portionSize)
          if (sourceHash != destinationHash) {
            isOrderPreserved = false
          }
        }
      } finally {
        sourceScanner.close()
        destinationScanner.close()
      }

      println(" - Destination topic preserves ordering of Source topic: %B".format(isOrderPreserved))

    } finally {
      if (out != null) {
        out.close()
      }
    }
  }

  private def getCount(table: HTable, hbaseConfig: Configuration, columnFamily: String, field: Array[Byte], value: Array[Byte]): Long = {
    val scan = new Scan
    scan.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("test_id"))
    scan.addColumn(Bytes.toBytes(columnFamily), field)
    val testFilter = new SingleColumnValueFilter(Bytes.toBytes(columnFamily), Bytes.toBytes("test_id"), CompareOp.EQUAL, Bytes.toBytes(config.testId))
    val topicFilter = new SingleColumnValueFilter(Bytes.toBytes(columnFamily), field, CompareOp.EQUAL, value)
    scan.setFilter(new FilterList(testFilter, topicFilter))
    val scanner = table.getScanner(scan)
    var total = 0
    var result: Result = scanner.next()
    while (result != null) {
      total = total + 1
      result = scanner.next()
    }

    total
  }

  private def calculateMD5ForSlice(it: java.util.Iterator[Result], portionSize: Int): String = {
    val bytes = new ByteArrayOutputStream()
    var left = portionSize
    while (it.hasNext && left > 0) {
      bytes.write(it.next().getValue(Bytes.toBytes(columnFamily), Bytes.toBytes("payload")))
      left = left - 1
    }

    new String(MessageDigest.getInstance("MD5").digest(bytes.toByteArray))
  }
}
