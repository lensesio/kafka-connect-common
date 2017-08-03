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

package com.datamountaineer.streamreactor.connect.config.base.traits

import com.datamountaineer.connector.config.{Config, FormatType, WriteModeEnum}
import com.datamountaineer.streamreactor.connect.rowkeys.{StringGenericRowKeyBuilder, StringKeyBuilder, StringStructFieldsStringKeyBuilder}
import com.datamountaineer.streamreactor.connect.config.base.const.TraitConfigConst.KCQL_PROP_SUFFIX
import org.apache.kafka.common.config.ConfigException

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

trait KcqlSettings extends BaseSettings {
  val kcqlConstant: String = s"$connectorPrefix.$KCQL_PROP_SUFFIX"

  def getKCQL: Set[Config] = {
    val raw = getString(kcqlConstant)
    if (raw.isEmpty) {
      throw new ConfigException(s"Missing $kcqlConstant")
    }
    raw.split(";").map(r => Config.parse(r)).toSet
  }

  def getKCQLRaw: Array[String] = {
    val raw = getString(kcqlConstant)
    if (raw.isEmpty) {
      throw new ConfigException(s"Missing $kcqlConstant")
    }
    raw.split(";")
  }

  def getFields(kcql: Set[Config] = getKCQL): Map[String, Map[String, String]] = {
    kcql.map(rm =>
      (rm.getSource, rm.getFieldAlias.map(fa => (fa.getField, fa.getAlias)).toMap)
    ).toMap
  }

  def getFieldsAliases(kcql: Set[Config] = getKCQL): Set[Map[String, String]] = {
    kcql.map(rm => rm.getFieldAlias.map(fa => (fa.getField, fa.getAlias)).toMap)
  }

  def getIgnoreFields(kcql: Set[Config] = getKCQL): Map[String, Set[String]] = {
    kcql.map(r => (r.getSource, r.getIgnoredField.toSet)).toMap
  }

  def getPrimaryKeys(kcql: Set[Config] = getKCQL): Map[String, Set[String]] = {
    kcql.map(r => (r.getSource, r.getPrimaryKeys.toSet)).toMap
  }

  def getTableTopic(kcql: Set[Config] = getKCQL): Map[String, String] = {
    kcql.map(r => (r.getSource, r.getTarget)).toMap
  }

  def getFormat(formatType: FormatType => FormatType, kcql: Set[Config] = getKCQL): Map[String, FormatType] = {
    kcql.map(r => (r.getSource, formatType(r.getFormatType))).toMap
  }

  def getTTL(kcql: Set[Config] = getKCQL): Map[String, Long] = {
    kcql.map(r => (r.getSource, r.getTTL)).toMap
  }

  def getIncrementalMode(kcql: Set[Config] = getKCQL): Map[String, String] = {
    kcql.map(r => (r.getSource, r.getIncrementalMode)).toMap
  }

  def getBatchSize(kcql: Set[Config] = getKCQL, defaultBatchSize: Int): Map[String, Int] = {
    kcql.map(r => (r.getSource, Option(r.getBatchSize).getOrElse(defaultBatchSize))).toMap
  }

  def getBucketSize(kcql: Set[Config] = getKCQL): Map[String, Int] = {
    kcql.map(r => (r.getSource, r.getBucketing.getBucketsNumber)).toMap
  }

  def getWriteMode(kcql: Set[Config] = getKCQL) : Map[String, WriteModeEnum] = {
    kcql.map(r => (r.getSource, r.getWriteMode)).toMap
  }

  def getAutoCreate(kcql: Set[Config] = getKCQL) : Map[String, Boolean] = {
    kcql.map(r => (r.getSource, r.isAutoCreate)).toMap
  }

  def getAutoEvolve(kcql: Set[Config] = getKCQL) : Map[String, Boolean] = {
    kcql.map(r => (r.getSource, r.isAutoEvolve)).toMap
  }

  def getUpsertKeys(kcql: Set[Config] = getKCQL): Map[String, Set[String]] = {
    kcql
      .filter(c => c.getWriteMode == WriteModeEnum.UPSERT)
      .map { r =>
        val keys = r.getPrimaryKeys.toSet
        if (keys.isEmpty) throw new ConfigException(s"${r.getTarget} is set up with upsert, you need primary keys setup")
        (r.getSource, keys)
      }.toMap
  }

  def getUpsertKey(kcql: Set[Config] = getKCQL): Map[String, String] = {
    kcql
      .filter(c => c.getWriteMode == WriteModeEnum.UPSERT)
      .map { r =>
        val keys = r.getPrimaryKeys.toSet
        if (keys.isEmpty) throw new ConfigException(s"${r.getTarget} is set up with upsert, you need primary keys setup")
        (r.getSource, keys.head)
      }.toMap
  }

  def getRowKeyBuilders(kcql: Set[Config] = getKCQL): Set[StringKeyBuilder] = {
    kcql.map { k =>
      val keys = k.getPrimaryKeys.asScala.toList
      // No PK => 'topic|par|offset' builder else generic-builder
      if (keys.nonEmpty) StringStructFieldsStringKeyBuilder(keys) else new StringGenericRowKeyBuilder()
    }
  }
}
