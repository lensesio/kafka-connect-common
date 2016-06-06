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

import com.fasterxml.jackson.databind.JsonNode
import io.confluent.connect.avro.AvroData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.json.{JsonConverter, JsonDeserializer}
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.storage.Converter

import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap

/**
  * Created by andrew@datamountaineer.com on 22/02/16. 
  * stream-reactor
  */

trait ConverterUtil {
  lazy val jsonConverter = new JsonConverter()
  lazy val deserializer = new JsonDeserializer()
  lazy val avroData = new AvroData(100)

  /**
    * Create a Struct based on a set of fields to extract from a ConnectRecord
    *
    * @param record The connectRecord to extract the fields from.
    * @param fields The fields to extract.
    * @param key Extract the fields from the key or the value of the ConnectRecord.
    * @return A new Struct with the fields specified in the fieldsMappings.
    * */
  def extractSinkFields(record: SinkRecord, fields: Map[String, String], key: Boolean = false) : SinkRecord = {
    //get the value
    val value : Struct = if (key) record.key().asInstanceOf[Struct] else record.value.asInstanceOf[Struct]

    if (fields.isEmpty) {
      record
    } else {
      //get the schema
      val currentSchema = if (key) record.keySchema() else record.valueSchema()
      val builder: SchemaBuilder = SchemaBuilder.struct.name(record.topic() + "_extracted")

      //build a new schema for the fields
      fields.foreach({
        case (name, alias) => {
          val extractedSchema = currentSchema.field(name)
          builder.field(alias, extractedSchema.schema())
        }
      })

      //build
      val extractedSchema = builder.build()

      //created new record.
      val newStruct = new Struct(extractedSchema)
      fields.foreach({
        case (name, alias) => {
          newStruct.put(alias, value.get(name))
        }
      })

      new SinkRecord(record.topic(), record.kafkaPartition(), Schema.STRING_SCHEMA, "key", extractedSchema, newStruct,
        record.kafkaOffset())
    }
  }

  /**
    * Convert a ConnectRecord value to a Json string using Kafka Connects deserializer
    *
    * @param record A ConnectRecord to extract the payload value from
    * @return A json string for the payload of the record
    * */
  def convertValueToJson(record: ConnectRecord) : JsonNode = {
    val converted: Array[Byte] = jsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value())
    deserializeToJson(record.topic(), payload = converted)
  }

  /**
    * Convert a ConnectRecord key to a Json string using Kafka Connects deserializer
    *
    * @param record A ConnectRecord to extract the payload value from
    * @return A json string for the payload of the record
    * */
  def convertKeyToJson(record: ConnectRecord) : JsonNode = {
    val converted = jsonConverter.fromConnectData(record.topic(), record.keySchema(), record.key())
    deserializeToJson(record.topic(), payload = converted)
  }

  /**
    * Deserialize Byte array for a topic to json
    *
    * @param topic Topic name for the byte array
    * @param payload Byte Array payload
    * @return A JsonNode representing the byte array
    * */
  def deserializeToJson(topic: String, payload: Array[Byte]) : JsonNode = {
    val json = deserializer.deserialize(topic, payload).get("payload")
    json
  }

  /**
    * Configure the converter
    *
    * @param converter The Converter to configure
    * @param props The props to configure with
    * */
  def configureConverter(converter: Converter, props: HashMap[String, String] = new HashMap[String, String] ) = {
    converter.configure(props.asJava, false)
  }

  /**
    * Convert SinkRecord to GenericRecord
    *
    * @param record ConnectRecord to convert
    * @return a GenericRecord
    **/
  def convertValueToGenericAvro(record: ConnectRecord): GenericRecord = {
    val avro = avroData.fromConnectData(record.valueSchema(), record.value())
    avro.asInstanceOf[GenericRecord]
  }
}
