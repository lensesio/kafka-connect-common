package com.datamountaineer.streamreactor.connect.offsets

import java.util
import java.util.Collections
import org.apache.kafka.connect.source.SourceTaskContext

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 25/04/16.
  * stream-reactor
  */
object OffsetHandler {

  /**
    * Recover the offsets
    *
    * @param lookupPartitionKey A partition key for the offset map
    * @param sourcePartition A list of datasets .i.e tables to get the partition offsets for
    * @param context The Source task context to get the offsets from
    * @return a List of partition offsets for the datasets
    * */
  def recoverOffsets(lookupPartitionKey: String, sourcePartition: util.List[String], context: SourceTaskContext) = {
    val partitions = sourcePartition.asScala.map(t => Collections.singletonMap(lookupPartitionKey, t)).asJava
    context.offsetStorageReader().offsets(partitions)
  }

  /**
    * Returns a last stored offset for the partitionKeyValue
    *
    * @param offsets The offsets to search through.
    * @param lookupPartitionKey The key for this partition .i.e. cassandra.assigned.tables.
    * @param partitionKeyValue The value for the partition .i.e. table1.
    * @param lookupOffsetCol The offset columns to look for. For example the timestamp column from table1.
    * @return The optional T of last stored value for the framework.
    * */
  def recoverOffset[T](offsets: util.Map[util.Map[String, String],util.Map[String, Object]],
                    lookupPartitionKey: String,
                    partitionKeyValue: String,
                    lookupOffsetCol: String
                   ) : Option[T] = {
    val partition = Collections.singletonMap(lookupPartitionKey, partitionKeyValue)
    val offset = offsets.get(partition)
    if (offset != null && offset.get(lookupOffsetCol) != null) {
      Some(offset.get(lookupOffsetCol).asInstanceOf[T])
    } else {
      None
    }
  }
}

