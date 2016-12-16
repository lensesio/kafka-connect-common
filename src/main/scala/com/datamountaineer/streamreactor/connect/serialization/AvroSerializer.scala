package com.datamountaineer.streamreactor.connect.serialization

import java.io.{ByteArrayOutputStream, InputStream, OutputStream}

import com.sksamuel.avro4s.{RecordFormat, SchemaFor}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}

object AvroSerializer {
  def write[T <: Product](t: T)(implicit os: OutputStream, formatter: RecordFormat[T], schemaFor: SchemaFor[T]): Unit = write(apply(t), schemaFor())

  def write(record: GenericRecord, schema: Schema)(implicit os: OutputStream) = {
    val writer = new GenericDatumWriter[GenericRecord](schema)
    val encoder = EncoderFactory.get().binaryEncoder(os, null)

    writer.write(record, encoder)
    encoder.flush()
    os.flush()
  }

  def getBytes[T <: Product](t: T)(implicit recordFormat: RecordFormat[T], schemaFor: SchemaFor[T]): Array[Byte] = getBytes(recordFormat.to(t), schemaFor())

  def getBytes(record: GenericRecord, schema: Schema): Array[Byte] = {
    implicit val output = new ByteArrayOutputStream()
    write(record, schema)
    output.toByteArray
  }

  def read(is: InputStream, schema: Schema): GenericRecord = {
    val reader = new GenericDatumReader[GenericRecord](schema)
    val decoder = DecoderFactory.get().binaryDecoder(is, null)
    reader.read(null, decoder)
  }

  def read[T <: Product](is: InputStream)(implicit schemaFor: SchemaFor[T], recordFormat: RecordFormat[T]): T = recordFormat.from(read(is, schemaFor()))

  def apply[T <: Product](t: T)(implicit formatter: RecordFormat[T]): GenericRecord = formatter.to(t)
}