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

package com.datamountaineer.streamreactor.connect.config

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.ConfigException

/**
  * Created by andrew@datamountaineer.com on 13/05/16. 
  * kafka-connect-common
  */

object Helpers extends StrictLogging {

  /**
    * Build a mapping of table to topic
    * filtering on the assigned tables.
    *
    * @param input The raw input string to parse .i.e. table:topic,table2:topic2.
    * @param filterTable The tables to filter for.
    * @return a Map of table->topic.
    * */
  def buildRouteMaps(input: String, filterTable: List[String]) : Map[String, String] = {
    tableTopicParser(input).filter({ case (k, v) => filterTable.contains(k)})
  }

  /**
    * Parser a route map {topic:table;f1->,f2->col} or {topic:table;*}
    *
    * @param routeMapping The route map string to parse form a sinks configuration.
    * @return A Export map containing the topic to table mapping and field to columns
    * */
  def mappingParser(routeMapping : String, pks: Option[String] = None) : List[RouteMapping] = {

    val pksMap = if (pks.isDefined && pks.get != null) pKParser(pks.get) else Map.empty[String, List[String]]

    //break into map of mappings
    val mappings = routeMapping.split("\\}")
      .toList
      .map(s => s.replace(",{", "").replace("{", "").replace("}", "").trim())

    //for each mapping extract the table to topic and field mappings
    mappings.map(m => {
      //split into two, table/topic and fields
      val topicField = m.split(";")

      require(topicField.size.equals(2), s"Invalid input while parsing $routeMapping, No topic routing or field mappings" +
        s". Required format is {source:target;field->target}. * from all fields")

      //parse table topics
      val tt = splitter(topicField(0), ":")

      require(tt.size > 0, s"Invalid input while parsing $m. No source to target mapping found. Required format is " +
        s"{source:target;field->target}. * from all fields")
      val source = tt.head._1
      val target = tt.head._2

      //now fields, is all fields?
      if (topicField(1).equals("*")) {
        logger.info(s"All fields to be selected from the source $source")
        //add primary keys
        val fields = if (pksMap.contains(source)){
          val pks = pksMap.get(source).get
          logger.info(s"Adding primary key field ${pks.mkString(",")}")
          pks.map(f=>Field(f, f, true))
        } else {
          List.empty
        }
        RouteMapping(source = source, target = target, allFields = true, fieldMappings = fields)
      } else {
        //split field to target field
        val fm: Map[String, String] = splitter(topicField(1), "->")
        logger.info(s"Applying fields selection ${fm.mkString(",")}")

        //check primary keys are in selection
        if (!pks.isEmpty) {
          if (pksMap.contains(source)) {
            val pks = pksMap.get(source).get
            pks.foreach(p=> {
              if (!fm.contains(p)) {
                throw new ConfigException(s"Primary key [$p] specified but is not in the field selection $m")
              }
            })
          }
        }

        ///add any primary keys
        val fields = fm.map(
          {
            case (name, target) => {
              if (pksMap.isEmpty) {
                Field(name, target)
              } else {
                //we have a pk, see if defined and set as primary
                if (pksMap.contains(source)) {
                  val pks = pksMap.get(source).get
                  Field(name, target, pks.contains(name))
                } else {
                  Field(name, target)
                }
              }
            }
          }
        ).toList

        RouteMapping(source = source, target = target, allFields = false, fieldMappings = fields)
      }
    })
  }

  //{table:f1,f2}
  def pKParser(input : String) : Map[String, List[String]] = {
    val mappings = input.split("\\}")
      .toList
      .map(s => s.replace(",{", "").replace("{", "").replace("}", "").trim())

    mappings.map(
      m => {
        val colon = m.indexOf(":")
        if (colon >= 0) {
          val topic = m.substring(0, colon)
          val fields = m.substring(colon + 1, m.length).split(",").toList
          (topic, fields)
        } else {
          throw new ConfigException(s"Invalid format for PKs. Received $input. Format should be {topic:f1,2}," +
            s"{topic2:f3,f3}....")
        }
      }
    ).toMap
  }

  /**
    * Break a comma and colon separated string into a map of table to topic or topic to table
    *
    * If now values is found after a comma the value before the comma is used.
    *
    * @param input The input string to parse.
    * @return a Map of table->topic or topic->table.
    * */
  def splitter(input: String, delimiter: String) : Map[String, String] = {
    input.split(",")
      .toList
      .map(c => c.split(delimiter))
      .map(a => {if (a.length == 1) (a(0), a(0)) else (a(0), a(1)) }).toMap
  }
  
  /**
    * Break a comma and colon separated string into a map of table to topic or topic to table
    *
    * If now values is found after a comma the value before the comma is used.
    *
    * @param input The input string to parse.
    * @return a Map of table->topic or topic->table.
    * */
  def tableTopicParser(input: String) : Map[String, String] = {
    input.split(",")
      .toList
      .map(c => c.split(":"))
      .map(a => {if (a.length == 1) (a(0), a(0)) else (a(0), a(1)) }).toMap
  }
}
