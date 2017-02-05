package com.datamountaineer.streamreactor.connect.converters

import java.util.Collections

import com.datamountaineer.streamreactor.connect.converters.source.MsgKey
import com.sksamuel.avro4s.{RecordFormat, SchemaFor}
import io.confluent.connect.avro.AvroData
import org.apache.avro.Schema
import org.apache.kafka.connect.data.Struct
import org.scalatest.{Matchers, WordSpec}

class JsonConverterWithSchemaEvolutionTest extends WordSpec with Matchers {
  val topic = "the_real_topic"
  val sourceTopic = "source_topic"
  val avroData = new AvroData(4)

  "JsonConverter" should {
    "throw IllegalArgumentException if payload is null" in {
      intercept[IllegalArgumentException] {
        val converter = new JsonConverterWithSchemaEvolution
        val record = converter.convert("topic", "somesource", 1000, null)
      }
    }

    "handle a simple json" in {
      val json = JacksonJson.toJson(Car("LaFerrari", "Ferrari", 2015, 963, 0.0001))
      val converter = new JsonConverterWithSchemaEvolution
      val record = converter.convert(topic, sourceTopic, 100, json.getBytes)
      record.keySchema() shouldBe MsgKey.schema
      record.key().asInstanceOf[Struct].getString("topic") shouldBe sourceTopic
      record.key().asInstanceOf[Struct].getInt32("id") shouldBe 100

      val schema =
        new Schema.Parser().parse(
          SchemaFor[CarOptional]().toString
            .replace("\"name\":\"CarOptional\"", s"""\"name\":\"$sourceTopic\"""")
            .replace(s""",\"namespace\":\"${getClass.getCanonicalName.dropRight(getClass.getSimpleName.length+1)}\"""", "")
        )
      val format = RecordFormat[CarOptional]
      val carOptional = format.to(CarOptional(Option("LaFerrari"), Option("Ferrari"), Option(2015), Option(963), Option(0.0001)))

      record.valueSchema() shouldBe avroData.toConnectSchema(schema)

      record.value() shouldBe avroData.toConnectData(schema, carOptional).value()
      record.sourcePartition() shouldBe null
      record.sourceOffset() shouldBe Collections.singletonMap(JsonConverterWithSchemaEvolution.ConfigKey, avroData.fromConnectSchema(avroData.toConnectSchema(schema)).toString())
    }
  }
}


case class Car(name: String,
               manufacturer: String,
               model: Long,
               bhp: Long,
               price: Double)


case class CarOptional(name: Option[String],
                       manufacturer: Option[String],
                       model: Option[Long],
                       bhp: Option[Long],
                       price: Option[Double])