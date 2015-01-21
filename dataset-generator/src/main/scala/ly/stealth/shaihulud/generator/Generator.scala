package ly.stealth.shaihulud.generator

import java.io.{BufferedOutputStream, FileOutputStream, OutputStream, File}

import scala.annotation.tailrec
import scala.util.Random

object Main {
  def main(args: Array[String]) {
    val start = System.currentTimeMillis()
    val config = parseConfig(args)
    Generator(config).generate()
    println(s"Done in ${(System.currentTimeMillis() - start)/1000} s")
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
    }

    parser.parse(args, GeneratorConfig()) match {
      case Some(config) => config
      case None => sys.exit(1)
    }
  }
}

case class GeneratorConfig(filename: String = "dataset", filesize: Long = 0, minLength: Int = 0, maxLength: Int = 0)

case class Generator(config: GeneratorConfig) {
  def generate() {
    val file = new File(this.config.filename)
    file.delete()
    file.createNewFile()
    val out = new BufferedOutputStream(new FileOutputStream(file))

    try {
      write(out)
    } finally {
      out.close()
    }
  }

  @tailrec
  private def write(out: OutputStream, currentSize: Long = 0) {
    if (currentSize < this.config.filesize) {
      val bytes = (Random.nextString(this.nextEntryLength()) + "\n").getBytes("UTF-8")
      out.write(bytes)
      write(out, currentSize + bytes.length)
    }
  }

  private def nextEntryLength(): Int = {
    this.config.minLength + Random.nextInt((this.config.maxLength - this.config.minLength) + 1)
  }
}
