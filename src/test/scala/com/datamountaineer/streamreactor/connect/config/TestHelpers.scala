package com.datamountaineer.streamreactor.connect.config

import org.apache.kafka.common.config.ConfigException
import org.scalatest.{Matchers, WordSpec}

/**
  * Created by andrew@datamountaineer.com on 18/05/16.
  * kafka-connect-common
  */
class TestHelpers extends WordSpec with Matchers {
  "should parse export map with no pks" in {
    val exportMapRaw = "{topic1:tableA;*}"
    val mappings: List[RouteMapping] = Helpers.mappingParser(exportMapRaw)
    mappings.size shouldBe 1
    mappings.foreach(m => {
      m.source shouldBe "topic1"
      m.target shouldBe "tableA"
      m.allFields shouldBe true
      m.fieldMappings.isEmpty shouldBe true
    })

    val exportMapRaw2 = "{topic1:tableA;f->,f2->col2}"
    val mappings2: List[RouteMapping] = Helpers.mappingParser(exportMapRaw2)
    mappings2.size shouldBe 1
    mappings2.foreach(m => {
      m.source shouldBe "topic1"
      m.target shouldBe "tableA"
      m.allFields shouldBe false
      m.fieldMappings.size shouldBe 2
      m.fieldMappings(0).name shouldBe "f"
      m.fieldMappings(0).target shouldBe "f"
      m.fieldMappings(1).name shouldBe "f2"
      m.fieldMappings(1).target shouldBe "col2"
    })

    val exportMapRaw3 = "{topic1:tableA;f->,f2->col2},{topic2:tableB;f->,f2->col2}"
    val mappings3: List[RouteMapping] = Helpers.mappingParser(exportMapRaw3)
    mappings3.size shouldBe 2
    mappings3(0).source shouldBe "topic1"
    mappings3(0).target shouldBe "tableA"
    mappings3(0).allFields shouldBe false
    mappings3(0).fieldMappings.size shouldBe 2
    mappings3(0).fieldMappings(0).name shouldBe "f"
    mappings3(0).fieldMappings(1).name shouldBe "f2"
    mappings3(0).fieldMappings.size shouldBe 2
    mappings3(0).fieldMappings(0).target shouldBe "f"
    mappings3(0).fieldMappings(1).target shouldBe "col2"
    mappings3(1).source shouldBe "topic2"
    mappings3(1).target shouldBe "tableB"
    mappings3(1).allFields shouldBe false
    mappings3(1).fieldMappings.size shouldBe 2
    mappings3(1).fieldMappings(0).name shouldBe "f"
    mappings3(1).fieldMappings(1).name shouldBe "f2"
    mappings3(1).fieldMappings(0).target shouldBe "f"
    mappings3(1).fieldMappings(1).target shouldBe "col2"
  }

  "should return two topics with pks set" in {
    //with PK
    val exportMapRaw4 = "{topic1:tableA;f->,f2->col2},{topic2:tableB;f->,f2->col2}"
    val pkMappings = "{topic1:f,f2},{topic2:f,f2}"
    val mappings4: List[RouteMapping] = Helpers.mappingParser(exportMapRaw4, Some(pkMappings))
    mappings4.size shouldBe 2
    mappings4(0).source shouldBe "topic1"
    mappings4(0).target shouldBe "tableA"
    mappings4(0).allFields shouldBe false
    mappings4(0).fieldMappings.size shouldBe 2
    mappings4(0).fieldMappings(0).name shouldBe "f"
    mappings4(0).fieldMappings(1).name shouldBe "f2"
    mappings4(0).fieldMappings.size shouldBe 2
    mappings4(0).fieldMappings(0).target shouldBe "f"
    mappings4(0).fieldMappings(1).target shouldBe "col2"
    mappings4(1).source shouldBe "topic2"
    mappings4(1).target shouldBe "tableB"
    mappings4(1).allFields shouldBe false
    mappings4(1).fieldMappings.size shouldBe 2
    mappings4(1).fieldMappings(0).name shouldBe "f"
    mappings4(1).fieldMappings(1).name shouldBe "f2"
    mappings4(1).fieldMappings(0).target shouldBe "f"
    mappings4(1).fieldMappings(1).target shouldBe "col2"

    mappings4(0).fieldMappings(0).isPrimaryKey shouldBe true
    mappings4(0).fieldMappings(1).isPrimaryKey shouldBe true

    mappings4(1).fieldMappings(0).isPrimaryKey shouldBe true
    mappings4(1).fieldMappings(1).isPrimaryKey shouldBe true

  }

  "should return a on topic with pks and the second not" in {
    //with only one PK'd topic
    val exportMapRaw5 = "{topic1:tableA;f->,f2->col2},{topic2:tableB;f->,f2->col2}"
    val pkMappings2 = "{topic1:f,f2}"
    val mappings5: List[RouteMapping] = Helpers.mappingParser(exportMapRaw5, Some(pkMappings2))
    mappings5.size shouldBe 2
    mappings5(0).source shouldBe "topic1"
    mappings5(0).target shouldBe "tableA"
    mappings5(0).allFields shouldBe false
    mappings5(0).fieldMappings.size shouldBe 2
    mappings5(0).fieldMappings(0).name shouldBe "f"
    mappings5(0).fieldMappings(1).name shouldBe "f2"
    mappings5(0).fieldMappings.size shouldBe 2
    mappings5(0).fieldMappings(0).target shouldBe "f"
    mappings5(0).fieldMappings(1).target shouldBe "col2"
    mappings5(1).source shouldBe "topic2"
    mappings5(1).target shouldBe "tableB"
    mappings5(1).allFields shouldBe false
    mappings5(1).fieldMappings.size shouldBe 2
    mappings5(1).fieldMappings(0).name shouldBe "f"
    mappings5(1).fieldMappings(1).name shouldBe "f2"
    mappings5(1).fieldMappings(0).target shouldBe "f"
    mappings5(1).fieldMappings(1).target shouldBe "col2"

    mappings5(0).fieldMappings(0).isPrimaryKey shouldBe true
    mappings5(0).fieldMappings(1).isPrimaryKey shouldBe true

    mappings5(1).fieldMappings(0).isPrimaryKey shouldBe false
    mappings5(1).fieldMappings(1).isPrimaryKey shouldBe false

  }
    //missing pk field in selection
    //with only one PK'd topic
  "should throw when pks not in selected fields" in {

    val exportMapRaw6 = "{topic1:tableA;f->,f2->col2},{topic2:tableB;f->,f2->col2}"
    val pkMappings6 = "{topic1:f9999}"

    intercept[ConfigException] {
      Helpers.mappingParser(exportMapRaw6, Some(pkMappings6))
    }


  }

  "should set primary keys when all fields are selected" in {
    val exportMapRaw7 = "{topic1:tableA;*},{topic2:tableB;f->,f2->col2}"
    val pkMappings3 ="{topic1:f,f2}"
    val mappings6: List[RouteMapping] = Helpers.mappingParser(exportMapRaw7, Some(pkMappings3))
    mappings6.size shouldBe 2
    mappings6(0).source shouldBe "topic1"
    mappings6(0).target shouldBe "tableA"
    mappings6(0).allFields shouldBe true
    mappings6(0).fieldMappings.size shouldBe 2
    mappings6(0).fieldMappings(0).name shouldBe "f"
    mappings6(0).fieldMappings(1).name shouldBe "f2"
    mappings6(0).fieldMappings.size shouldBe 2
    mappings6(0).fieldMappings(0).target shouldBe "f"
    mappings6(0).fieldMappings(1).target shouldBe "f2"
    mappings6(1).source shouldBe "topic2"
    mappings6(1).target shouldBe "tableB"
    mappings6(1).allFields shouldBe false
    mappings6(1).fieldMappings.size shouldBe 2
    mappings6(1).fieldMappings(0).name shouldBe "f"
    mappings6(1).fieldMappings(1).name shouldBe "f2"
    mappings6(1).fieldMappings(0).target shouldBe "f"
    mappings6(1).fieldMappings(1).target shouldBe "col2"

    mappings6(0).fieldMappings(0).isPrimaryKey shouldBe true
    mappings6(0).fieldMappings(1).isPrimaryKey shouldBe true

    mappings6(1).fieldMappings(0).isPrimaryKey shouldBe false
    mappings6(1).fieldMappings(1).isPrimaryKey shouldBe false
  }
}
