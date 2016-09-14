package com.datamountaineer.streamreactor.connect.schemas

import com.datamountaineer.streamreactor.connect.TestUtilsBase
import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 29/02/16. 
  * stream-reactor
  */
class TestConverterUtil extends TestUtilsBase with ConverterUtil {

  "ConverterUtil" should {
    "Should convert a SinkRecord Value to json" in {
      val testRecord = getTestRecord
      val json = convertValueToJson(testRecord).toString
      json shouldBe VALUE_JSON_STRING
    }

    "Should convert a SinkRecord Key to json" in {
      val testRecord = getTestRecord
      val json = convertKeyToJson(testRecord).asText()
      json shouldBe KEY
    }

    "Should convert a SinkRecord Key to avro" in {
      val testRecord = getTestRecord
      val avro = convertValueToGenericAvro(testRecord)
      val testAvro = buildAvro()
      avro.get("id") shouldBe testAvro.get("id")
      avro.get("int_field") shouldBe testAvro.get("int_field")
      avro.get("long_field") shouldBe testAvro.get("long_field")
      avro.get("string_field") shouldBe testAvro.get("string_field")
    }

    "Should return a subset SinkRecord" in {
      val testRecord = getTestRecord
      val converted = convert(testRecord, Map("id"->"id", "int_field"->"int_field"))
      val fields = converted.valueSchema().fields().asScala.map(f=>f.name()).toSet
      fields.contains("id") shouldBe true
      fields.contains("int_field") shouldBe true
      fields.contains("long_field") shouldBe false
    }

    "Should return a ignore fields SinkRecord" in {
      val testRecord = getTestRecord
      val converted = convert(testRecord, Map.empty[String, String], Set("long_field"))
      val fields = converted.valueSchema().fields().asScala.map(f=>f.name()).toSet
      fields.contains("long_field") shouldBe false
    }

    "Should return same SinkRecord" in {
      val testRecord = getTestRecord
      val converted = convert(testRecord, Map.empty[String, String], Set.empty[String])
      converted shouldBe testRecord
    }
  }
}
