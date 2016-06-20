package com.datamountaineer.streamreactor.connect.schemas

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.confluent.kafka.schemaregistry.client.rest.RestService

import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 13/06/16. 
  * kafka-connect-common
  */
object SchemaRegistry extends StrictLogging {

  /**
    * Get a schema for a given subject
    *
    * @param url The url of the schema registry
    * @param subject The subject to het the schema for
    * @return The schema for the subject
    * */
  def getSchema(url : String, subject : String) : String = {
    val registry = new RestService(url)

    Try(registry.getLatestVersion(subject).getSchema) match {
      case Success(s) => {
        logger.info(s"Found schema for $subject")
        s
      }
      case Failure(f) => {
        logger.warn("Unable to connect to the Schema registry. An attempt will be made to create the table" +
          " on receipt of the first records.")
        ""
      }
    }
  }

  /**
    * Get a list of subjects from the registry
    *
    * @param url The url to the schema registry
    * @return A list of subjects/topics
    * */
  def getSubjects(url: String) : List[String] = {
    val registry = new RestService(url)
    val schemas: List[String] = Try(registry.getAllSubjects.asScala.toList) match {
      case Success(s) => s
      case Failure(f) => {
        logger.warn("Unable to connect to the Schema registry. An attempt will be made to create the table" +
          " on receipt of the first records.")
        List.empty[String]
      }
    }

    schemas.foreach(s=>logger.info(s"Found schemas for $s"))
    schemas
  }
}
