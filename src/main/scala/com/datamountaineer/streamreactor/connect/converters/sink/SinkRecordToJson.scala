package com.datamountaineer.streamreactor.connect.converters.source

import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import org.json4s.jackson.JsonMethods._

/**
  * Created by andrew@datamountaineer.com on 29/12/2016. 
  * kafka-connect-common
  */
object SinkRecordToJson extends ConverterUtil {

  private val mapper = new ObjectMapper()

  def apply(record: SinkRecord,
            fields : Map[String, Map[String, String]],
            ignoreFields : Map[String, Set[String]]): String = {

    val schema = record.valueSchema()
    val value = record.value()

    if (schema == null) {
      //try to take it as string
      value match {
        case map: java.util.Map[_, _] =>
          val extracted = convertSchemalessJson(record,
            fields.getOrElse(record.topic(), Map.empty),
            ignoreFields.getOrElse(record.topic(), Set.empty))
            .asInstanceOf[java.util.Map[String, Any]]
          //not ideal; but the implementation is hashmap anyway
          mapper.writeValueAsString(extracted)

        case _ => sys.error("For schemaless record only String and Map types are supported")
      }
    } else {
      schema.`type`() match {
        case Schema.Type.STRING =>
          val extracted = convertStringSchemaAndJson(record,
            fields.getOrElse(record.topic(), Map.empty),
            ignoreFields.getOrElse(record.topic(), Set.empty))
          compact(render(extracted))
        case Schema.Type.STRUCT =>
          val extracted = convert(record,
            fields.getOrElse(record.topic(), Map.empty),
            ignoreFields.getOrElse(record.topic(), Set.empty))

          simpleJsonConverter.fromConnectData(extracted.valueSchema(), extracted.value()).toString

        case other => sys.error(s"$other schema is not supported")
      }
    }
  }
}

