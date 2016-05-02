package com.datamountaineer.streamreactor.connect.queues

import java.util
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import com.google.common.collect.Queues

/**
  * Created by r on 3/1/16.
  */
object QueueHelpers {

  implicit class LinkedBlockingQueueExtension[T](val lbq: LinkedBlockingQueue[T]) extends AnyVal {
    def drainWithTimeoutTo(collection: util.Collection[_ >: T], maxElements: Int, timeout: Long, unit: TimeUnit): Int = {
      Queues.drain[T](lbq, collection, maxElements, timeout, unit)
    }
  }

  /**
    * Drain the queue with timeout
    *
    * @param queue The queue to drain
    * @param batchSize Batch size to take
    * @param timeOut Timeout to take the batch
    * @return ArrayList of T
    * */
  def drainQueueWithTimeOut[T](queue: LinkedBlockingQueue[T], batchSize: Int, timeOut: Long) = {
    val l = new util.ArrayList[T]()
    queue.drainWithTimeoutTo(l, batchSize, (timeOut * 1E9).toLong, TimeUnit.NANOSECONDS)
    l
  }

  /**
    * Drain the queue
    *
    * @param queue The queue to drain
    * @param batchSize Batch size to take
    * @return ArrayList of T
    * */
  def drainQueue[T](queue: LinkedBlockingQueue[T], batchSize: Int) = {
    val l = new util.ArrayList[T]()
    //val drainSize: Int = if (batchSize > queue.size()) queue.size() else batchSize
    queue.drainTo(l, batchSize)
    l
  }
}
