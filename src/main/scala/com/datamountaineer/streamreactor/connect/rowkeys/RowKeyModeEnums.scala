package com.datamountaineer.streamreactor.connect.rowkeys

/**
  * Created by andrew@datamountaineer.com on 27/05/16. 
  * kafka-connect-common
  */
object RowKeyModeEnum extends Enumeration {
  type RowKeyModeEnum = Value
  val FIELDS, GENERIC, SINKRECORD, AVRO = Value
}
