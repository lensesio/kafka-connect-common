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

import com.datamountaineer.streamreactor.connect.json.SimpleJsonConverter
import com.fasterxml.jackson.databind.JsonNode
import io.confluent.connect.avro.{AvroConverter, AvroData}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data._
import org.apache.kafka.connect.json.JsonDeserializer
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.storage.Converter

import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap

/**
  * Created by andrew@datamountaineer.com on 22/02/16. 
  * stream-reactor
  */


trait ConverterUtil {
  type avroSchema = org.apache.avro.Schema

  lazy val simpleJsonConverter = new SimpleJsonConverter()
  lazy val deserializer = new JsonDeserializer()
  lazy val avroConverter = new AvroConverter()
  lazy val avroData = new AvroData(100)

  /**
    * Create a Struct based on a set of fields to extract from a ConnectRecord
    *
    * @param record       The connectRecord to extract the fields from.
    * @param fields       The fields to extract.
    * @param ignoreFields Fields to ignore from the sink records.
    * @param key          Extract the fields from the key or the value of the ConnectRecord.
    * @return A new Struct with the fields specified in the fieldsMappings.
    **/
  def convert(record: SinkRecord,
              fields: Map[String, String],
              ignoreFields: Set[String] = Set.empty[String],
              key: Boolean = false): SinkRecord = {
    val value: Struct = if (key) record.key().asInstanceOf[Struct] else record.value.asInstanceOf[Struct]

    if (fields.isEmpty && ignoreFields.isEmpty) {
      record
    } else {
      val currentSchema = if (key) record.keySchema() else record.valueSchema()
      val builder: SchemaBuilder = SchemaBuilder.struct.name(record.topic() + "_extracted")

      //build a new schema for the fields
      if (fields.nonEmpty) {
        fields.foreach({ case (name, alias) =>
          val extractedSchema = currentSchema.field(name)
          builder.field(alias, extractedSchema.schema())
        })
      } else if (ignoreFields.nonEmpty) {
        val ignored = currentSchema.fields().asScala.filterNot(f => ignoreFields.contains(f.name()))
        ignored.foreach(i => builder.field(i.name, i.schema))
      } else {
        currentSchema.fields().asScala.foreach(f => builder.field(f.name(), f.schema()))
      }

      val extractedSchema = builder.build()
      val newStruct = new Struct(extractedSchema)
      fields.foreach({ case (name, alias) => newStruct.put(alias, value.get(name)) })

      new SinkRecord(record.topic(), record.kafkaPartition(), Schema.STRING_SCHEMA, "key", extractedSchema, newStruct,
        record.kafkaOffset())
    }
  }

  /**
    * Convert a ConnectRecord value to a Json string using Kafka Connects deserializer
    *
    * @param record A ConnectRecord to extract the payload value from
    * @return A json string for the payload of the record
    **/
  def convertValueToJson(record: ConnectRecord): JsonNode = {
    simpleJsonConverter.fromConnectData(record.valueSchema(), record.value())
  }

  /**
    * Convert a ConnectRecord key to a Json string using Kafka Connects deserializer
    *
    * @param record A ConnectRecord to extract the payload value from
    * @return A json string for the payload of the record
    **/
  def convertKeyToJson(record: ConnectRecord): JsonNode = {
    simpleJsonConverter.fromConnectData(record.keySchema(), record.key())
  }

  /**
    * Deserialize Byte array for a topic to json
    *
    * @param topic   Topic name for the byte array
    * @param payload Byte Array payload
    * @return A JsonNode representing the byte array
    **/
  def deserializeToJson(topic: String, payload: Array[Byte]): JsonNode = {
    val json = deserializer.deserialize(topic, payload).get("payload")
    json
  }

  /**
    * Configure the converter
    *
    * @param converter The Converter to configure
    * @param props     The props to configure with
    **/
  def configureConverter(converter: Converter, props: HashMap[String, String] = new HashMap[String, String]) = {
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

  def convertAvroToConnect(topic: String, obj: Array[Byte]) = avroConverter.toConnectData(topic, obj)
}
