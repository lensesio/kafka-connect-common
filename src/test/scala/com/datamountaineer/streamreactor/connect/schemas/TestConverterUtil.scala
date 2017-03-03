/*
 *  Copyright 2017 Datamountaineer.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.schemas

import java.util

import com.datamountaineer.streamreactor.connect.TestUtilsBase
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.json.JsonConverter
import org.apache.kafka.connect.sink.SinkRecord
import org.json4s.jackson.JsonMethods._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 29/02/16. 
  * stream-reactor
  */
class TestConverterUtil extends TestUtilsBase with ConverterUtil {

  "ConverterUtil" should {
    "convert a SinkRecord Value to json" in {
      val testRecord = getTestRecord
      val json = convertValueToJson(testRecord).toString
      json shouldBe VALUE_JSON_STRING
    }

    "convert a SinkRecord Key to json" in {
      val testRecord = getTestRecord
      val json = convertKeyToJson(testRecord).asText()
      json shouldBe KEY
    }

    "convert a SinkRecord Key to avro" in {
      val testRecord = getTestRecord
      val avro = convertValueToGenericAvro(testRecord)
      val testAvro = buildAvro()
      avro.get("id") shouldBe testAvro.get("id")
      avro.get("int_field") shouldBe testAvro.get("int_field")
      avro.get("long_field") shouldBe testAvro.get("long_field")
      avro.get("string_field") shouldBe testAvro.get("string_field")
    }

    "return a subset SinkRecord" in {
      val testRecord = getTestRecord
      val converted = convert(testRecord, Map("id" -> "id", "int_field" -> "int_field"))
      val fields = converted.valueSchema().fields().asScala.map(f => f.name()).toSet
      fields.contains("id") shouldBe true
      fields.contains("int_field") shouldBe true
      fields.contains("long_field") shouldBe false
    }

    "return a ignore fields SinkRecord" in {
      val testRecord = getTestRecord
      val converted = convert(testRecord, Map.empty[String, String], Set("long_field"))
      val fields = converted.valueSchema().fields().asScala.map(f => f.name()).toSet
      fields.contains("long_field") shouldBe false
    }

    "return same SinkRecord" in {
      val testRecord = getTestRecord
      val converted = convert(testRecord, Map.empty[String, String], Set.empty[String])
      converted shouldBe testRecord
    }

    "throw an error while converting schemaless record if the payload is not Map[String, Any]" in {
      intercept[RuntimeException] {
        val record = new SinkRecord("t", 0, null, null, null, "Should not be here", 0)
        convertSchemalessJson(record, Map.empty)
      }
    }

    "remove the specified field when converting a schemaless record" in {
      val map = new util.HashMap[String, Any]()
      map.put("field1", "value1")
      map.put("field2", 3)
      map.put("toremove", null)
      val record = new SinkRecord("t", 0, null, null, null, map, 0)
      convertSchemalessJson(record, Map.empty, Set("toremove")).asScala shouldBe Map("field1" -> "value1", "field2" -> 3)
    }


    "only select the fields specified when converting a schemaless sink with the value being a json" in {
      val map = new util.HashMap[String, Any]()
      map.put("field1", "value1")
      map.put("field2", 3)
      map.put("field3", null)
      val record = new SinkRecord("t", 0, null, null, null, map, 0)
      convertSchemalessJson(record, Map("field1" -> "field1", "field2" -> "fieldRenamed"), Set.empty, includeAllFields = false).asScala shouldBe Map(
        "field1" -> "value1", "fieldRenamed" -> 3
      )
    }


    "rename the specified field when converting a schemaless record" in {
      val map = new util.HashMap[String, Any]()
      map.put("field1", "value1")
      map.put("field2", 3)
      map.put("field3", null)
      val record = new SinkRecord("t", 0, null, null, null, map, 0)
      convertSchemalessJson(record, Map("field1" -> "field1", "field2" -> "fieldRenamed"), Set("toremove")).asScala shouldBe Map(
        "field1" -> "value1", "fieldRenamed" -> 3, "field3" -> null
      )
    }

    "convert a json via JsonConverter and then apply a field alias and one remove " in {
      val converter = new JsonConverter()
      converter.configure(Map("schemas.enable" -> false), false)

      val schemaAndValue = converter.toConnectData("topicA",
        """
          |{
          |    "id": 1,
          |    "name": "A green door",
          |    "price": 12.50,
          |    "tags": ["home", "green"]
          |}
        """.stripMargin.getBytes)

      val map = schemaAndValue.value().asInstanceOf[java.util.Map[String, Any]].asScala
      map shouldBe Map("id" -> 1, "name" -> "A green door", "price" -> 12.5, "tags" -> List("home", "green").asJava)

      val result = convertSchemalessJson(new SinkRecord("topicA", 0, null, null, null, schemaAndValue.value, 0),
        Map("id" -> "id", "tags" -> "tagsRenamed"), Set("price"), includeAllFields = false).asScala

      result shouldBe Map("id" -> 1, "tagsRenamed" -> List("home", "green").asJava)
    }


    "throw an error while converting a json payload" in {
      intercept[RuntimeException] {
        val record = new SinkRecord("t", 0, null, null, null, Map.empty[String, String], 0)
        convertSchemalessJson(record, Map.empty)
      }
    }

    "remove the specified field when converting a json for a record with Schema.String" in {
      val json =
        """
          |{
          |   "field1":"value1",
          |   "field2":3,
          |   "toremove":""
          |}
        """.stripMargin

      val record = new SinkRecord("t", 0, null, null, Schema.STRING_SCHEMA, json, 0)
      val actual = compact(render(convertStringSchemaAndJson(record, Map.empty, Set("toremove"))))

      actual shouldBe "{\"field1\":\"value1\",\"field2\":3}"
    }


    "only select the fields specified when converting a record with Schema.String and payload a json string" in {
      val json =
        """
          |{
          |   "field1":"value1",
          |   "field2":3,
          |   "field3":""
          |}
        """.stripMargin

      val record = new SinkRecord("t", 0, null, null, Schema.STRING_SCHEMA, json, 0)

      val actual = compact(render(convertStringSchemaAndJson(record, Map("field1" -> "field1", "field2" -> "fieldRenamed"), Set.empty, includeAllFields = false)))
      val expected ="{\"field1\":\"value1\",\"fieldRenamed\":3}"

      actual shouldBe expected
    }


    "rename the specified field when converting a record with Schema.String and value is json" in {
      val json =
        """
          |{
          |   "field1":"value1",
          |   "field2":3,
          |   "field3":""
          |}
        """.stripMargin

      val record = new SinkRecord("t", 0, null, null, Schema.STRING_SCHEMA, json, 0)
      val actual = compact(render(convertStringSchemaAndJson(record, Map("field1" -> "field1", "field2" -> "fieldRenamed"), Set("toremove"))))
      val expected ="{\"field1\":\"value1\",\"fieldRenamed\":3,\"field3\":\"\"}"
      actual shouldBe expected
    }

    "apply a field alias and one remove when converting a sink record with Schema.String and the payload a json" in {
      val json =
        """
          |{
          |    "id": 1,
          |    "name": "A green door",
          |    "price": 12.50,
          |    "tags": ["home", "green"]
          |}
        """.stripMargin

      val actual = compact(render(convertStringSchemaAndJson(new SinkRecord("topicA", 0, null, null, Schema.STRING_SCHEMA, json, 0),
        Map("id" -> "id", "tags" -> "tagsRenamed"), Set("price"), includeAllFields = false)))

      val expected ="{\"id\":1,\"tagsRenamed\":[\"home\",\"green\"]}"
      actual shouldBe expected
    }
  }
}
