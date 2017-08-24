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

import java.util

import com.datamountaineer.kcql.{Field, FormatType, Kcql, WriteModeEnum}
import com.datamountaineer.streamreactor.connect.rowkeys.{StringGenericRowKeyBuilder, StringKeyBuilder, StringStructFieldsStringKeyBuilder}
import com.datamountaineer.streamreactor.connect.config.base.const.TraitConfigConst.KCQL_PROP_SUFFIX
import org.apache.kafka.common.config.ConfigException

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

trait KcqlSettings extends BaseSettings {
  val kcqlConstant: String = s"$connectorPrefix.$KCQL_PROP_SUFFIX"

  def getKCQL: Set[Kcql] = {
    val raw = getString(kcqlConstant)
    if (raw.isEmpty) {
      throw new ConfigException(s"Missing $kcqlConstant")
    }
    raw.split(";").map(r => Kcql.parse(r)).toSet
  }

  def getKCQLRaw: Array[String] = {
    val raw = getString(kcqlConstant)
    if (raw.isEmpty) {
      throw new ConfigException(s"Missing $kcqlConstant")
    }
    raw.split(";")
  }

  def getFieldsMap(kcql: Set[Kcql] = getKCQL): Map[String, Map[String, String]] = {
    kcql.map(rm =>
      (rm.getSource, rm.getFields.map(fa => (fa.getName, fa.getAlias)).toMap)
    ).toMap
  }

  def getFields(kcql: Set[Kcql] = getKCQL): Map[String, Seq[Field]] = {
    kcql.map(rm => (rm.getSource, rm.getFields.asScala)).toMap
  }

  def getIgnoreFields(kcql: Set[Kcql] = getKCQL): Map[String, Seq[Field]] = {
    kcql.map(rm => (rm.getSource, rm.getIgnoredFields.asScala)).toMap
  }

  def getFieldsAliases(kcql: Set[Kcql] = getKCQL): Set[Map[String, String]] = {
    kcql.map(rm => rm.getFields.map(fa => (fa.getName, fa.getAlias)).toMap)
  }

  def getIgnoreFieldsMap(kcql: Set[Kcql] = getKCQL): Map[String, Set[String]] = {
    kcql.map(r => (r.getSource,  r.getIgnoredFields.map(f => f.getName).toSet)).toMap
  }

  def getPrimaryKeys(kcql: Set[Kcql] = getKCQL): Map[String, Set[String]] = {
    kcql.map(r => (r.getSource, r.getPrimaryKeys.map(f => f.getName).toSet)).toMap
  }

  def getTableTopic(kcql: Set[Kcql] = getKCQL): Map[String, String] = {
    kcql.map(r => (r.getSource, r.getTarget)).toMap
  }

  def getFormat(formatType: FormatType => FormatType, kcql: Set[Kcql] = getKCQL): Map[String, FormatType] = {
    kcql.map(r => (r.getSource, formatType(r.getFormatType))).toMap
  }

  def getTTL(kcql: Set[Kcql] = getKCQL): Map[String, Long] = {
    kcql.map(r => (r.getSource, r.getTTL)).toMap
  }

  def getIncrementalMode(kcql: Set[Kcql] = getKCQL): Map[String, String] = {
    kcql.map(r => (r.getSource, r.getIncrementalMode)).toMap
  }

  def getBatchSize(kcql: Set[Kcql] = getKCQL, defaultBatchSize: Int): Map[String, Int] = {
    kcql.map(r => (r.getSource, Option(r.getBatchSize).getOrElse(defaultBatchSize))).toMap
  }

  def getBucketSize(kcql: Set[Kcql] = getKCQL): Map[String, Int] = {
    kcql.map(r => (r.getSource, r.getBucketing.getBucketsNumber)).toMap
  }

  def getWriteMode(kcql: Set[Kcql] = getKCQL) : Map[String, WriteModeEnum] = {
    kcql.map(r => (r.getSource, r.getWriteMode)).toMap
  }

  def getAutoCreate(kcql: Set[Kcql] = getKCQL) : Map[String, Boolean] = {
    kcql.map(r => (r.getSource, r.isAutoCreate)).toMap
  }

  def getAutoEvolve(kcql: Set[Kcql] = getKCQL) : Map[String, Boolean] = {
    kcql.map(r => (r.getSource, r.isAutoEvolve)).toMap
  }

  def getUpsertKeys(kcql: Set[Kcql] = getKCQL): Map[String, Set[String]] = {
    kcql
      .filter(c => c.getWriteMode == WriteModeEnum.UPSERT)
      .map { r =>
        val keys = r.getPrimaryKeys.map(k => k.getName).toSet
        if (keys.isEmpty) throw new ConfigException(s"${r.getTarget} is set up with upsert, you need primary keys setup")
        (r.getSource, keys)
      }.toMap
  }

  def getUpsertKey(kcql: Set[Kcql] = getKCQL): Map[String, String] = {
    kcql
      .filter(c => c.getWriteMode == WriteModeEnum.UPSERT)
      .map { r =>
        val keys = r.getPrimaryKeys.toSet
        if (keys.isEmpty) throw new ConfigException(s"${r.getTarget} is set up with upsert, you need primary keys setup")
        (r.getSource, keys.head.getName)
      }.toMap
  }

  def getRowKeyBuilders(kcql: Set[Kcql] = getKCQL): Set[StringKeyBuilder] = {
    kcql.map { k =>
      val keys = k.getPrimaryKeys.asScala.map(k => k.getName)
      // No PK => 'topic|par|offset' builder else generic-builder
      if (keys.nonEmpty) StringStructFieldsStringKeyBuilder(keys) else new StringGenericRowKeyBuilder()
    }
  }

  def getPrimaryKeyCols(kcql: Set[Kcql] = getKCQL): Map[String, Set[String]] = {
    kcql.map(k => (k.getSource, k.getPrimaryKeys.map(p => p.getName).toSet)).toMap
  }

  def getIncrementalMode(routes: Seq[Kcql]): Map[String, String] = {
    routes.map(r => (r.getSource, r.getIncrementalMode)).toMap
  }

  def checkInputTopics(props: Map[String, String]) = {
    val topics = props.get("topics").get.split(",").toSet
    val raw = props.get(kcqlConstant).get
    if (raw.isEmpty) {
      throw new ConfigException(s"Missing $kcqlConstant")
    }
    val kcql = raw.split(";").map(r => Kcql.parse(r)).toSet
    val sources = kcql.map(k => k.getSource)

    val res = topics.subsetOf(sources)

    if (!res) {
      throw new ConfigException(s"Mandatory `topics` configuration contains topics not set in $kcqlConstant")
    }

    val res1 = sources.subsetOf(topics)

    if (!res1) {
      throw new ConfigException(s"$kcqlConstant configuration contains topics not set in mandatory `topic` configuration")
    }

    true
  }
}
