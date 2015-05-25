package ly.stealth.latencies

import java.nio.ByteBuffer

import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties
import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic.{GenericRecord, GenericDatumReader}
import org.apache.avro.io.DecoderFactory
import org.apache.avro.util.Utf8
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.errors.SerializationException

class AvroDecoder(props: VerifiableProperties) extends Decoder[SchemaAndData] {
  var schemaRegistry: SchemaRegistryClient = null
  if (props == null) {
    throw new ConfigException("Missing schema registry url!")
  } else {
    val url = props.getProperty("schema.registry.url")
    if (url == null) {
      throw new ConfigException("Missing schema registry url!")
    } else {
      val maxSchemaObject = props.getInt("max.schemas.per.subject", 1000)
      schemaRegistry = new CachedSchemaRegistryClient(url, maxSchemaObject)
    }
  }

  def fromBytes(bytes: Array[Byte]): SchemaAndData = {
    val payload = getByteBuffer(bytes)
    val schemaId = payload.getInt

    SchemaAndData(schemaRegistry.getByID(schemaId), payload)
  }

  def getByteBuffer(payload: Array[Byte]): ByteBuffer = {
    val buffer = ByteBuffer.wrap(payload)
    if(buffer.get() != 0) {
      throw new SerializationException("Unknown magic byte!");
    } else {
      buffer
    }
  }
}

case class SchemaAndData(schema: Schema, data: ByteBuffer) {

  def deserialize(): AnyRef = {
    val messageLength = data.limit() - 1 - 4
    if (schema.getType == Schema.Type.BYTES) {
      val bytes = new Array[Byte](messageLength)
      data.get(bytes, 0, messageLength)
      bytes
    } else {
      val start = data.position() + data.arrayOffset()
      val reader = new GenericDatumReader[GenericRecord](schema)
      val obj: AnyRef = reader.read(null, DecoderFactory.get().binaryDecoder(data.array(), start, messageLength, null))
      if (schema.getType.equals(Type.STRING)) {
        obj.asInstanceOf[Utf8].toString
      } else {
        obj
      }
    }
  }
}
