package com.datamountaineer.streamreactor.connect.sink

import org.apache.kafka.connect.sink.SinkRecord


/**
  * Defines the construct for inserting a new row for the connect sink record
  */
trait DbWriter extends AutoCloseable {
  def write(records: Seq[SinkRecord]): Unit
}
