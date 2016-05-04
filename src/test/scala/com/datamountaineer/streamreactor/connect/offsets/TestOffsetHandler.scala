package com.datamountaineer.streamreactor.connect.offsets

import com.datamountaineer.streamreactor.connect.TestUtilsBase
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, WordSpec}
import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 28/04/16. 
  * kafka-connect-common
  */
/**
  * Created by andrew@datamountaineer.com on 27/04/16.
  * stream-reactor
  */
class TestOffsetHandler extends WordSpec with Matchers with MockitoSugar with TestUtilsBase {
  "should return an offset" in {
    val lookupPartitionKey = "test_lk_key"
    val offsetValue = "2013-01-01 00:05+0000"
    val offsetColumn = "my_timeuuid_col"
    val table = "testTable"
    val taskContext = getSourceTaskContext(lookupPartitionKey, offsetValue,offsetColumn, table)

    //check we can read it back
    val tables = List(table)
    val offsetsRecovered = OffsetHandler.recoverOffsets(lookupPartitionKey, tables.asJava, taskContext)
    val offsetRecovered = OffsetHandler.recoverOffset[String](offsetsRecovered, lookupPartitionKey, table, offsetColumn)
    offsetRecovered.get shouldBe (offsetValue)
  }
}
