/*
 * Copyright 2012-2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.edda

import scala.actors.Actor
import scala.actors.TIMEOUT

import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

import com.netflix.servo.monitor.Monitors
import com.netflix.servo.DefaultMonitorRegistry

import com.amazonaws.AmazonClientException

import org.slf4j.LoggerFactory

/** local state for Crawlers
  *
  * @param records the records that were crawled
  */
case class CrawlerState(records: Seq[Record] = Seq[Record]())

/** companion object for [[com.netflix.edda.Crawler]].  */
object Crawler extends StateMachine.LocalState[CrawlerState] {

  /** Message sent to Observers */
  case class CrawlResult(from: Actor, newRecordSet: RecordSet)(implicit req: RequestId) extends StateMachine.Message {
    override def toString = s"CrawlResult(req=$req, newRecords=${newRecordSet.records.size} meta=${newRecordSet.meta})"
  }

  /** Message to start a crawl action */
  case class Crawl(from: Actor)(implicit req: RequestId) extends StateMachine.Message

}

/** Crawler to crawl something and generate Records based on what was crawled.
  * Those records are then passed to a Collection (typically) by sending the
  * crawl results to all observers.
 */
abstract class Crawler extends Observable {

  import Crawler._
  import Utils._

  private[this] val logger = LoggerFactory.getLogger(getClass)
  lazy val enabled = Utils.getProperty("edda.crawler", "enabled", name, "true")

  /* initial delay in ms between rapid, successive api requests */
  var request_delay = Utils.getProperty("edda.crawler", "requestDelay", name, "0").get.toInt
  /* delay iterator for each request when the API limit is reached */
  val throttle_delay = Utils.getProperty("edda.crawler", "throttleDelay", name, "200").get.toInt
  /* number of retries attempted */
  var retry_count = 0
  /* maximum number of retries before giving up */
  val retry_max = Utils.getProperty("edda.crawler", "maxDelayMultiplier", name, "225").get.toInt
  /* using a ratio of 2:1 for errors to retries due to requests being sent concurrently */
  val errorReducer = 2


  /** start a crawl if the crawler is enabled */
  def crawl()(implicit req: RequestId) {
    if (enabled.get.toBoolean) {
      val msg = Crawl(Actor.self)
      if (logger.isDebugEnabled) logger.debug(s"$req${Actor.self} sending: $msg -> $this")
      this ! msg
    }
  }

  /** see [[com.netflix.edda.Observable.addObserver]].  Overridden to be a NoOp when Crawler is not enabled */
  override def addObserver(actor: Actor)(implicit req: RequestId): scala.concurrent.Future[StateMachine.Message] = {
    import ObserverExecutionContext._
    if (enabled.get.toBoolean) super.addObserver(actor) else scala.concurrent.future {
      Observable.OK(Actor.self)
    }
  }

  /** see [[com.netflix.edda.Observable.delObserver]].  Overridden to be a NoOp when Crawler is not enabled */
  override def delObserver(actor: Actor)(implicit req: RequestId): scala.concurrent.Future[StateMachine.Message] = {
    import ObserverExecutionContext._
    if (enabled.get.toBoolean) super.delObserver(actor) else scala.concurrent.future {
      Observable.OK(Actor.self)
    }
  }

  /** name of the Crawler, typically matches the name of the Collection that the Crawler works with */
  def name: String

  // basic servo metrics
  private[this] val crawlTimer = Monitors.newTimer("crawl")
  private[this] val crawlCounter = Monitors.newCounter("crawl.count")
  private[this] val errorCounter = Monitors.newCounter("crawl.errors")

  /** abstract routine for subclasses to implement the actual crawl logic */
  protected def doCrawl()(implicit req: RequestId): Seq[Record]

  /** initilize the initial Crawler state */
  protected override def initState = addInitialState(super.initState, newLocalState(CrawlerState()))

  /** init just registers metrics for Servo */
  protected override def init() {
    Monitors.registerObject("edda.crawler." + name, this)
    DefaultMonitorRegistry.getInstance().register(Monitors.newThreadPoolMonitor(s"edda.crawler.$name.threadpool", this.pool.asInstanceOf[ThreadPoolExecutor]))
    super.init
  }

  /** handle Crawl Messages to the StateMachine */
  private def localTransitions: PartialFunction[(Any, StateMachine.State), StateMachine.State] = {
    case (gotMsg @ Crawl(from), state) => {
      implicit val req = gotMsg.req
      // this is blocking so we don't crawl in parallel

      // in case we are crawling slower than expected
      // we might have a bunch of Crawl messages in the
      // mailbox, so just burn through them now
      flushMessages {
        case Crawl(from) => true
      }
      val stopwatch = crawlTimer.start()
      val newRecords = try {
        doCrawl()
      } catch {
        case e: Exception => {
          errorCounter.increment()
          throw e
        }
      } finally {
        stopwatch.stop()
      }

      if (logger.isInfoEnabled) logger.info("{} {} Crawled {} records in {} sec", toObjects(
        req, this, newRecords.size, stopwatch.getDuration(TimeUnit.MILLISECONDS) / 1000D -> "%.2f"))
      crawlCounter.increment(newRecords.size)
      Observable.localState(state).observers.foreach(o => {
          val msg = Crawler.CrawlResult(this, RecordSet(newRecords, Map("source" -> "crawl", "req" -> req.id)))
          if (logger.isDebugEnabled) logger.debug(s"$req$this sending: $msg -> $o")
          o ! msg
      })
      /* reset the error count at the end of each run */
      retry_count = 0
      setLocalState(state, CrawlerState(records = newRecords))

      // } else state
    }
  }

  def backoffRequest[T](code: =>T): T = {
    if (request_delay > 0) Thread sleep request_delay
    try {
      if (retry_count > 10) {
        if (logger.isDebugEnabled) logger.debug(s"$this SLEEPING [" + retry_count.toString + "]: " + ( throttle_delay * (((retry_count-10)/errorReducer) + 1) ).toString)
        Thread sleep ( throttle_delay * (((retry_count-10)/errorReducer) + 1) )
      }
      code
    } catch {
      case e: AmazonClientException => {
        val pattern = ".*Error Code: ([A-Za-z]+);.*".r
        val pattern(err_code) = e.getMessage()
        if ( (err_code == "RequestLimitExceeded") || (err_code == "Throttling") ) {
          Thread sleep ( throttle_delay * ((retry_count/errorReducer) + 1) )
          retry_count = retry_count + 1
          if ((retry_count/errorReducer) >= retry_max) {
            logger.error("Hit configured maximum number of API backoff requests, aborting")
            throw e
          }
          backoffRequest { code }
        } else {
          logger.error("Unexpected AmazonClientException, aborting")
          throw e
        }
      }
      case e: Exception => {
        logger.error("Unexpected Exception, aborting")
        throw e
      }
    }
  }

  protected override def transitions = localTransitions orElse super.transitions

  override def toString = "[Crawler " + name + "]"
}
