package com.datamountaineer.streamreactor.connect.sink

import com.datamountaineer.streamreactor.connect.rowkeys.StringStructFieldsStringKeyBuilder
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.{Matchers, WordSpec}


class StringStructFieldsStringKeyBuilderTest extends WordSpec with Matchers {
  "StructFieldsStringKeyBuilder" should {
    "raise an exception if the field is not present in the struct" in {
      intercept[IllegalArgumentException] {
        val schema = SchemaBuilder.struct().name("com.example.Person")
          .field("firstName", Schema.STRING_SCHEMA)
          .field("age", Schema.INT32_SCHEMA)
          .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build()

        val struct = new Struct(schema).put("firstName", "Alex").put("age", 30)

        val sinkRecord = new SinkRecord("sometopic", 1, null, null, schema, struct, 1)
        StringStructFieldsStringKeyBuilder(Seq("threshold")).build(sinkRecord)
      }
    }

    "create the row key based on one single field in the struct" in {
      val schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build()

      val struct = new Struct(schema).put("firstName", "Alex").put("age", 30)

      val sinkRecord = new SinkRecord("sometopic", 1, null, null, schema, struct, 1)
      StringStructFieldsStringKeyBuilder(Seq("firstName")).build(sinkRecord) shouldBe "Alex"
    }

    "create the row key based on more thant one field in the struct" in {
      val schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build()

      val struct = new Struct(schema).put("firstName", "Alex").put("age", 30)

      val sinkRecord = new SinkRecord("sometopic", 1, null, null, schema, struct, 1)
      StringStructFieldsStringKeyBuilder(Seq("firstName", "age")).build(sinkRecord) shouldBe "Alex.30"
    }
  }
}