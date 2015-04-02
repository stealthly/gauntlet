package ly.stealth.shaihulud.reader

import java.io.{ByteArrayOutputStream, FileOutputStream, PrintStream}
import java.security.MessageDigest

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient
import org.apache.hadoop.hbase.client.{Result, Scan, Get, HTable}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.util.Bytes

class Validator(config: ReaderConfiguration) extends Serializable {
  val columnFamily = "kafka_client_validator"

  def validate() {
    var out: PrintStream = null
    try {
      out = new PrintStream(new FileOutputStream("gauntlet-test-%s.txt".format(config.testId)), true)
      System.setOut(out)

      val hbaseConfig = HBaseConfiguration.create()
      hbaseConfig.set("hbase.zookeeper.quorum", config.zookeeper)
      //hbaseConfig.set("hbase.zookeeper.property.clientPort", 10000)

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

      val portionSize = 1000
      val testFilter = new SingleColumnValueFilter(Bytes.toBytes(columnFamily), Bytes.toBytes("test_id"), CompareOp.EQUAL, Bytes.toBytes(config.testId))
      val sourceFilter = new SingleColumnValueFilter(Bytes.toBytes(columnFamily), Bytes.toBytes("source_topic"), CompareOp.EQUAL, source)
      val destinationFilter = new SingleColumnValueFilter(Bytes.toBytes(columnFamily), Bytes.toBytes("destination_topic"), CompareOp.EQUAL, destination)
      val sourcePageFilter = new ColumnPaginationFilter(portionSize, 0)
      val destinationPageFilter = new ColumnPaginationFilter(portionSize, 0)

      val sourceScan = new Scan(Bytes.toBytes(columnFamily))
      sourceScan.setFilter(new FilterList(testFilter, sourceFilter, sourcePageFilter))
      val sourceScanner = messagesTable.getScanner(sourceScan)
      val si = sourceScanner.iterator()

      val destinationScan = new Scan(Bytes.toBytes(columnFamily))
      destinationScan.setFilter(new FilterList(testFilter, destinationFilter, destinationPageFilter))
      val destinationScanner = messagesTable.getScanner(destinationScan)
      val di = destinationScanner.iterator()

      var isOrderPreserved = true
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
      out = new PrintStream(new FileOutputStream("gauntlet-test-%s.txt".format(config.testId)), true)
      System.setOut(out)
    }
  }

  private def getCount(table: HTable, hbaseConfig: Configuration, columnFamily: String, field: Array[Byte], value: Array[Byte]): Long = {
    val aggregationClient = new AggregationClient(hbaseConfig)
    val scan = new Scan()
    scan.addFamily(Bytes.toBytes(columnFamily))
    val testFilter = new SingleColumnValueFilter(Bytes.toBytes(columnFamily), Bytes.toBytes("test_id"), CompareOp.EQUAL, Bytes.toBytes(config.testId))
    val topicFilter = new SingleColumnValueFilter(Bytes.toBytes(columnFamily), field, CompareOp.EQUAL, value)
    scan.setFilter(new FilterList(testFilter, topicFilter))

    aggregationClient.rowCount(table, null, scan)
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
