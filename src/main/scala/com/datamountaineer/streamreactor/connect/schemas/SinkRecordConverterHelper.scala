package com.datamountaineer.streamreactor.connect.schemas

import java.util

import com.datamountaineer.streamreactor.connect.converters.sink.SinkRecordToJson.{convertSchemalessJson, convertStringSchemaAndJson}
import com.datamountaineer.streamreactor.connect.schemas.StructHelper.StructExtension
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.header.ConnectHeaders
import org.apache.kafka.connect.sink.SinkRecord
import org.json4s.JsonAST.JObject

import scala.collection.JavaConverters._

object SinkRecordConverterHelper extends StrictLogging {

  implicit final class SinkRecordExtension(val record: SinkRecord)
      extends AnyVal {

    private def structFromHeaders(headerFields: Map[String, String]): Struct = {

      val extractHeaderFields =
        if (headerFields.nonEmpty && headerFields.contains("*")) {
          record
            .headers()
            .asScala
            .filter(h => h.schema().`type`() == Schema.STRING_SCHEMA.`type`())
            .map(h => (h.key() -> h.key()))
            .toMap
        } else {
          headerFields
        }

      val schemaBuilder = SchemaBuilder.struct()

      val headers =
        record
          .headers()
          .iterator()
          .asScala
          // only take string headers and those in our list of fields
          .filter(h =>
            h.schema().`type`() == Schema.STRING_SCHEMA
              .`type`() && extractHeaderFields.contains(h.key()))
          //get the alias
          .map(h => (extractHeaderFields(h.key()), h.value()))
          .map {
            case (name, value) =>
              schemaBuilder.field(name, Schema.STRING_SCHEMA)
              (name, value)
          }
          .toMap

      val newStruct = new Struct(schemaBuilder.build())
      headers.foreach { case (name, value) => newStruct.put(name, value) }
      newStruct
    }


    /**
     * make new sink record, taking fields
     * from the key, value and headers
     * */
    def newFilteredRecord(
        fields: Map[String, String],
        ignoreFields: Set[String] = Set.empty[String],
        keyFields: Map[String, String] = Map.empty[String, String],
        headerFields: Map[String, String] = Map.empty[String, String],
        retainKey: Boolean = false,
        retainHeaders: Boolean = false): SinkRecord = {

      //if we have keys fields and a key value extract
      val keyStruct = if (keyFields.nonEmpty && record.key() != null) {
        extract(payload = record.key(),
                payloadSchema = record.keySchema(),
                key = true,
                fields = keyFields,
                ignoreFields = Set.empty)
      } else {
        logger.warn(
          s"Key is null for topic [${record.topic()}], partition [${record
            .kafkaPartition()}], offset [${record.kafkaOffset()}])")
        new Struct(SchemaBuilder.struct().build())
      }

      //if we have value fields and a value extract
      val valueStruct = if (fields.nonEmpty && record.value() != null) {
        extract(payload = record.value(),
                payloadSchema = record.valueSchema(),
                key = false,
                fields = fields,
                ignoreFields = ignoreFields)
      } else {
        logger.warn(
          s"Value is null for topic [${record.topic()}], partition [${record
            .kafkaPartition()}], offset [${record.kafkaOffset()}])")
        new Struct(SchemaBuilder.struct().build())
      }

      //create a new struct with the keys, values and headers
      val struct = keyStruct ++ valueStruct ++ structFromHeaders(headerFields)

      new SinkRecord(
        record.topic(),
        record.kafkaPartition(),
        if (retainKey) record.keySchema() else null,
        if (retainKey) record.key() else null,
        struct.schema(),
        struct,
        record.kafkaOffset(),
        record.timestamp(),
        record.timestampType(),
        if (retainHeaders) record.headers() else new ConnectHeaders()
      )
    }

    private def extract(payload: Object,
                        payloadSchema: Schema,
                        key: Boolean,
                        fields: Map[String, String],
                        ignoreFields: Set[String]): Struct = {

      if (payloadSchema == null) {
        //json with no schema
        val j: util.Map[String, Any] = convertSchemalessJson(
          record = record,
          fields = fields,
          ignoreFields = ignoreFields,
          key = key,
          includeAllFields = fields.contains("*")
        )

        val newSchema = SchemaBuilder.struct()
        j.asScala
          .foreach {
            case (name, value) => newSchema.field(name, Schema.STRING_SCHEMA)
          }

        val newStruct = new Struct(newSchema.build())
        j.asScala.foreach {
          case (name, value) => newStruct.put(name, value.toString)
        }
        newStruct

      } else {
        payloadSchema.`type`() match {
          //struct
          case Schema.Type.STRUCT =>
            payload
              .asInstanceOf[Struct]
              .reduceToSchema(schema = payloadSchema,
                              fields = fields,
                              ignoreFields = ignoreFields)

          // json with string schema
          case Schema.Type.STRING =>
            val j = convertStringSchemaAndJson(record = record,
                                               fields = fields,
                                               ignoreFields = ignoreFields,
                                               key = key,
                                               includeAllFields =
                                                 fields.contains("*"))

            val newSchema = SchemaBuilder.struct()
            val jFields = j.asInstanceOf[JObject].values
            jFields.foreach {
              case (name, value) =>
                // default to string
                newSchema.field(name, Schema.STRING_SCHEMA)
            }
            val newStruct = new Struct(newSchema.build())
            jFields.foreach {
              case (name, value) =>
                newStruct.put(name, value.toString)
            }
            newStruct

          case other =>
            throw new ConnectException(
              s"$other schema is not supported for extracting fields")
        }
      }
    }
  }
}
