package com.datamountaineer.streamreactor.connect.config

import com.datamountaineer.streamreactor.connect.TestUtilsBase
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil

/**
  * Created by andrew@datamountaineer.com on 29/02/16. 
  * stream-reactor
  */
class TestConverterUtil extends TestUtilsBase with ConverterUtil {

  "ConverterUtil" should {
    "Should convert a SinkRecord Value to json" in {
      val testRecord = getTestRecord
      configureConverter(jsonConverter)
      val json = convertValueToJson(testRecord).toString
      json shouldBe VALUE_JSON_STRING
    }

    "Should convert a SinkRecord Key to json" in {
      val testRecord = getTestRecord
      configureConverter(jsonConverter)
      val json = convertKeyToJson(testRecord).asText()
      json shouldBe KEY
    }

    "Should convert a SinkRecord Key to avro" in {
      val testRecord = getTestRecord
      val avro = convertToGenericAvro(testRecord)
      val testAvro = buildAvro()
      avro.get("id") shouldBe testAvro.get("id")
      avro.get("int_field") shouldBe testAvro.get("int_field")
      avro.get("long_field") shouldBe testAvro.get("long_field")
      avro.get("string_field") shouldBe testAvro.get("string_field")
    }
  }
}
