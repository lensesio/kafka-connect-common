package com.datamountaineer.streamreactor.connect.errors

import java.text.SimpleDateFormat
import java.util.Date

import com.typesafe.scalalogging.slf4j.StrictLogging

import scala.util.{Failure, Success, Try}

/**
  * Created by andrew@datamountaineer.com on 29/05/16. 
  * stream-reactor-maven
  */
trait ErrorHandler extends StrictLogging {
  var errorTracker : Option[ErrorTracker] = None
  private val dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS'Z'")

  def initialize(maxRetries: Int, errorPolicy: ErrorPolicy): Unit = {
    errorTracker = Some(ErrorTracker(maxRetries, maxRetries, "", new Date(), errorPolicy))
  }

  def doTry[A](t :Try[A]) : Option[A] = {
    require(errorTracker.isDefined, "ErrorTracker is not set call. Initialize.")

    t
    match  {
      case Success(s) => {
        //success, check if we had previous errors.
        if (errorTracker.get.retries != errorTracker.get.maxRetries) {
          logger.info(s"Recovered from error ${errorTracker.get.lastErrorMessage} at " +
            s"${dateFormatter.format(errorTracker.get.lastErrorTimestamp)}")
        }
        //cleared error
        resetErrorTracker()
        Some(s)
      }
      case Failure(f) =>
        //decrement the retry count
        logger.error(s"Encountered error ${f.getMessage}")
        this.errorTracker = Some(decrementErrorTracker(errorTracker.get, f.getMessage))
        handleError(f, errorTracker.get.retries, errorTracker.get.policy)
        None
    }
  }

  def resetErrorTracker() = {
    errorTracker = Some(ErrorTracker(errorTracker.get.maxRetries, errorTracker.get.maxRetries, "", new Date(),
      errorTracker.get.policy))
  }

  private def decrementErrorTracker(errorTracker: ErrorTracker, msg : String) : ErrorTracker = {
    ErrorTracker(errorTracker.retries - 1, errorTracker.maxRetries, msg, new Date(), errorTracker.policy)
  }

  private def handleError(f : Throwable, retries: Int, policy: ErrorPolicy): Unit = {
    policy.handle(f, true, retries)
  }
}
