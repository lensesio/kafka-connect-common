/**
  * Copyright 2016 Datamountaineer.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  **/

package com.datamountaineer.streamreactor.connect.schemas

/**
  * Created by andrew@datamountaineer.com on 29/05/16. 
  * kafka-connect-common
  */

import java.text.SimpleDateFormat
import java.util.TimeZone

import org.apache.kafka.connect.data._

import scala.collection.JavaConversions._

trait StructFieldsValuesExtractor {
  def get(struct: Struct): Seq[(String, Any)]
}

/**
  * Extracts fields from a SinkRecord Struct based on a specified set of provided columns.
  *
  * @param includeAllFields Boolean indicating if all the fields from the SinkRecord are to be written to the sink
  * @param fieldsAliasMap   A map of fields and their alias,if provided, to extract from the SinkRecord
  **/
case class StructFieldsExtractor(includeAllFields: Boolean, fieldsAliasMap: Map[String, String]) extends StructFieldsValuesExtractor {

  /**
    * Get a sequence of columns names to column values for a given struct
    *
    * @param struct A SinkRecord struct
    * @return a Sequence of column names and values
    **/
  def get(struct: Struct): Seq[(String, AnyRef)] = {
    val schema = struct.schema()
    val fields: Seq[Field] = if (includeAllFields) schema.fields()
    else schema.fields().filter(f => fieldsAliasMap.contains(f.name()))

    val fieldsAndValues = fields.flatMap { case field =>
      getFieldValue(field, struct).map(value => fieldsAliasMap.getOrElse(field.name(), field.name()) -> value)
    }
    fieldsAndValues
  }

  /**
    * For a field in a struct return the value
    *
    * @param field  A field to return the value for
    * @param struct A struct to extract the field from
    * @return an optional value for the field
    **/
  private def getFieldValue(field: Field, struct: Struct): Option[AnyRef] = {
    Option(struct.get(field)) match {
      case None => None
      case Some(value) =>
        val fieldName = field.name()
        val v = field.schema() match {
          case Schema.BOOLEAN_SCHEMA | Schema.OPTIONAL_BOOLEAN_SCHEMA => struct.getBoolean(fieldName)
          case Schema.BYTES_SCHEMA | Schema.OPTIONAL_BYTES_SCHEMA => struct.getBytes(fieldName)
          case Schema.FLOAT32_SCHEMA | Schema.OPTIONAL_FLOAT32_SCHEMA => struct.getFloat32(fieldName)
          case Schema.FLOAT64_SCHEMA | Schema.OPTIONAL_FLOAT64_SCHEMA => struct.getFloat64(fieldName)
          case Schema.INT8_SCHEMA | Schema.OPTIONAL_INT8_SCHEMA => struct.getInt8(fieldName)
          case Schema.INT16_SCHEMA | Schema.OPTIONAL_INT16_SCHEMA => struct.getInt16(fieldName)
          case Schema.INT32_SCHEMA | Schema.OPTIONAL_INT32_SCHEMA => struct.getInt32(fieldName)
          case Schema.INT64_SCHEMA | Schema.OPTIONAL_INT64_SCHEMA => struct.getInt64(fieldName)
          case Schema.STRING_SCHEMA | Schema.OPTIONAL_STRING_SCHEMA => struct.getString(fieldName)
          case other =>
            field.schema().name() match {
              case Decimal.LOGICAL_NAME => Decimal.toLogical(field.schema, value.asInstanceOf[Array[Byte]])
              case Date.LOGICAL_NAME => StructFieldsExtractor.DateFormat.format(Date.toLogical(field.schema, value.asInstanceOf[Int]))
              case Time.LOGICAL_NAME => StructFieldsExtractor.TimeFormat.format(Time.toLogical(field.schema, value.asInstanceOf[Int]))
              case Timestamp.LOGICAL_NAME => StructFieldsExtractor.DateFormat.format(Timestamp.toLogical(field.schema, value.asInstanceOf[Long]))
              case _ => value
            }
        }
        Some(v)
    }
  }
}


object StructFieldsExtractor {
  val DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  val TimeFormat: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss.SSSZ")
  DateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
}