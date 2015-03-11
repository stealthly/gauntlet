package ly.stealth.shaihulud.generator

import java.io._
import java.nio.ByteBuffer

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumWriter, GenericData, GenericRecord}
import org.apache.avro.io.EncoderFactory

import scala.annotation.tailrec
import scala.util.Random

object Main {
  def main(args: Array[String]) {
    val start = System.currentTimeMillis()
    val config = parseConfig(args)
    Generator(config).generate()
    println(s"Done in ${(System.currentTimeMillis() - start) / 1000} s")
  }

  def parseConfig(args: Array[String]): GeneratorConfig = {
    val parser = new scopt.OptionParser[GeneratorConfig]("Generator") {
      head("Dataset Generator")
      opt[String]('f', "filename").text("Name of output file.").action {
        (value, config) =>
          config.copy(filename = value)
      }

      opt[Long]('s', "filesize").required().text("Max size of output file.").action {
        (value, config) =>
          config.copy(filesize = value)
      }

      opt[Int]('l', "min.length").required().text("Minimum length of generated value.").action {
        (value, config) =>
          config.copy(minLength = value)
      }

      opt[Int]('h', "max.length").required().text("Maximum length of generated value.").action {
        (value, config) =>
          config.copy(maxLength = value)
      }

      opt[File]('a', "avro.schema").text("Avro schema file to generate dataset for.").action {
        (value, config) =>
          config.copy(avroSchema = value)
      }

      opt[String]('g', "generator.class").text("Avro generator class.").action {
        (value, config) =>
          config.copy(avroGenerator = value)
      }
    }

    parser.parse(args, GeneratorConfig()) match {
      case Some(config) => {
        if ((config.avroGenerator.isEmpty && config.avroSchema != null) || (config.avroGenerator != "" && config.avroSchema == null)) {
          println("avro.schema and generator.class flags should be used in pair")
          sys.exit(1)
        }
        config
      }
      case None => sys.exit(1)
    }
  }
}

case class GeneratorConfig(filename: String = "dataset", filesize: Long = 0, minLength: Int = 0, maxLength: Int = 0, avroSchema: File = null, avroGenerator: String = "")

case class Generator(config: GeneratorConfig) {
  def generate() {
    val file = new File(this.config.filename)
    file.delete()
    file.createNewFile()
    val out = new BufferedOutputStream(new FileOutputStream(file))

    try {
      if (this.config.avroSchema == null) {
        write(randomLine, out)
      } else {
        val schema = new Schema.Parser().parse(this.config.avroSchema)
        val generator = Class.forName(this.config.avroGenerator).newInstance().asInstanceOf[AvroGenerator]
        write(generator.randomLine(this.config, schema), out)
      }
    } finally {
      out.close()
    }
  }

  @tailrec
  private def write(generator: () => Array[Byte], out: OutputStream, currentSize: Long = 0) {
    if (currentSize < this.config.filesize) {
      val bytes = generator()
      out.write(bytes)
      write(generator, out, currentSize + bytes.length)
    }
  }

  private def nextEntryLength(): Int = {
    this.config.minLength + Random.nextInt((this.config.maxLength - this.config.minLength) + 1)
  }

  def randomLine(): Array[Byte] = {
    (Random.nextString(this.nextEntryLength()) + "\n").getBytes("UTF-8")
  }
}

trait AvroGenerator {
  def randomLine(config: GeneratorConfig, schema: Schema): () => Array[Byte] = {
    () => {
      val record = new GenericData.Record(schema)
      populateRecord(config, record)

      val out = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get().directBinaryEncoder(out, null)
      new GenericDatumWriter[GenericRecord](schema).write(record, encoder)

      out.toByteArray ++ "\n".getBytes("UTF-8")
    }
  }

  def populateRecord(config: GeneratorConfig, record: GenericRecord)
}

class MetricLineGenerator extends AvroGenerator {
  def populateRecord(config: GeneratorConfig, record: GenericRecord) {
    record.put("id", 0L)
    record.put("timings", new java.util.ArrayList[Long]())
    record.put("value", ByteBuffer.wrap(Random.nextString((config.maxLength - config.minLength) + 1).getBytes("UTF-8")))
  }
}
