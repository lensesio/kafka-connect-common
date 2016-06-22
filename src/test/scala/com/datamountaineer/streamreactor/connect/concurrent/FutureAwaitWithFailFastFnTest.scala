package com.datamountaineer.streamreactor.connect.concurrent

import java.util.concurrent.Executors

import com.datamountaineer.streamreactor.connect.concurrent.ExecutorExtension._
import org.scalatest.concurrent.{Eventually, Timeouts}
import org.scalatest.time.{Millis, Span}
import org.scalatest.{Matchers, WordSpec}

import scala.util.{Failure, Try}

/**
  * Created by stepi on 22/06/16.
  */
class FutureAwaitWithFailFastFnTest extends WordSpec with Matchers with Eventually with Timeouts {
  "FutureAwaitWithFailFastFn" should {
    "return when all the futures have completed" in {
      val exec = Executors.newFixedThreadPool(10)
      val futures = (1 to 5).map(i => exec.submit {
        Thread.sleep(300)
        i
      })

      eventually {
        val result = FutureAwaitWithFailFastFn(exec, futures)
        exec.isTerminated shouldBe true
        result shouldBe Seq(1, 2, 3, 4, 5)
      }(PatienceConfig(Span(3000, Millis), Span(500, Millis)))
    }

    "stop when the first futures times out" in {
      val exec = Executors.newFixedThreadPool(6)
      val futures = for (i <- 1 to 10) yield {
        exec.submit {
          if (i == 4) {
            Thread.sleep(1000)
            sys.error("this task failed.")
          } else {
            Thread.sleep(50000)
          }
        }
      }

      eventually {
        val t = Try(FutureAwaitWithFailFastFn(exec, futures))
        t.isFailure shouldBe true
        t.asInstanceOf[Failure[_]].exception.getMessage shouldBe "this task failed."
        exec.isTerminated shouldBe true
      }(PatienceConfig(Span(5000, Millis), Span(500, Millis)))
    }
  }

}
