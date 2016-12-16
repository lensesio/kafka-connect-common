package com.datamountaineer.streamreactor.connect.converters.source

import java.util.Collections

import com.sksamuel.avro4s.{RecordFormat, SchemaFor}
import io.confluent.connect.avro.AvroData
import org.apache.avro.Schema
import org.scalatest.{Matchers, WordSpec}

class JsonSimpleConverterTest extends WordSpec with Matchers {
  val topic = "the_real_topic"
  val sourceTopic = "source_topic"
  val avroData = new AvroData(4)

  "JsonSimpleConverter" should {
    "convert from json to the struct" in {
      val car = Car("LaFerrari", "Ferrari", 2015, 963, 0.0001)
      val json = JacksonJson.toJson(car)
      val converter = new JsonSimpleConverter
      val record = converter.convert(topic, sourceTopic, 100, json.getBytes)
      record.keySchema() shouldBe MsgKey.schema
      record.key() shouldBe MsgKey.getStruct(sourceTopic, 100)

      val schema = new Schema.Parser().parse(
        SchemaFor[Car]().toString
          .replace("\"name\":\"Car\"", s"""\"name\":\"$sourceTopic\"""")
          .replace(s"""\"namespace\":\"${getClass.getCanonicalName.dropRight(getClass.getSimpleName.length+1)}\",""", "")
      )
      val format = RecordFormat[Car]
      val avro = format.to(car)

      record.valueSchema() shouldBe avroData.toConnectSchema(schema)

      record.value() shouldBe avroData.toConnectData(schema, avro).value()
      record.sourcePartition() shouldBe Collections.singletonMap(Converter.TopicKey, sourceTopic)
      record.sourceOffset() shouldBe null
    }
  }
}
