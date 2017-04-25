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

package com.datamountaineer.streamreactor.connect.queues

import java.util
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import com.google.common.collect.Queues
import com.typesafe.scalalogging.slf4j.StrictLogging

/**
  * Created by r on 3/1/16.
  */
object QueueHelpers extends StrictLogging {

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
    logger.debug(s"Found ${queue.size()}. Draining entries to batchSize ${batchSize}.")
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
    logger.debug(s"Found ${queue.size()}. Draining entries to batchSize ${batchSize}.")
    queue.drainTo(l, batchSize)
    l
  }
}
