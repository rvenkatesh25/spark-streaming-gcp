package com.thumbtack.common.model

import org.apache.spark.SparkConf
import org.apache.spark.streaming.scheduler._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.{Logger, LoggerFactory}

trait SparkStream[T] {
  def streamConfig: SparkStreamConfig[T]

  /**
    * Override this in your job if you need to set other
    * Spark Streaming config variables before running.
    */
  def sparkConfigVars: Map[String, String] = Map.empty[String, String]

  def batchDurationSeconds: Int = 1

  def localCheckPointPath: String = s"/tmp/sparkCheckPointDir/${this.getClass.getName}"
  def checkPointPath: String = s"hdfs:///user/spark/sparkCheckPointDir/${this.getClass.getName}"

  /** set to true for local testing */
  def isLocal: Boolean = false

  lazy val conf = {
    val c = new SparkConf().setAppName(this.getClass.getName)
    c.set("spark.ui.port", (4040 + scala.util.Random.nextInt(1000)).toString)
    c.set("spark.streaming.stopSparkContextByDefault", "false")

    // process queued batches before shutting down
    c.set("spark.streaming.stopGracefullyOnShutdown", "true")

    // resilience mode is enabled by default
    c.set("spark.streaming.receiver.writeAheadLog.enable", "true")

    // default locality wait is 3s before spark moves from process-local to
    // data-local to rack-local to any. this ends up scheduling all batches on
    // the receiver executor only, and moving on only if a single batch takes a
    // long time; eventually leading to increasing scheduling delays. setting
    // this to 0 favors parallelism over data locality, which is needed since
    // the data to be moved around is small but the http calls for output
    // operations are usually the slowest step
    c.set("spark.locality.wait", "0s")

    "spark.task.maxFailures"
    if (isLocal) {
      // for local testing access spark UI at
      // http://localhost:4158/jobs/
      c.set("spark.ui.port", "4158")
      c.setMaster("local[4]")
    }
    c.setAll(sparkConfigVars)
  }

  private def getCheckPointPath: String = {
    if (isLocal) localCheckPointPath else checkPointPath
  }

  final def newLogger(): Logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Main logic of the streaming job should be defined by overriding this method
    *
    * @param config Job specific configuration
    * @param ssc streaming context to attach the input source to
    */
  def main(config: T, ssc: StreamingContext): Unit

  private def createStreamingContext(config: T): StreamingContext = {
    val ssc = new StreamingContext(conf, Seconds(batchDurationSeconds))
    main(config, ssc)
    ssc.checkpoint(getCheckPointPath)
    ssc
  }
  "spark.scheduler.mode: FAIR

  /**
    * Entry point of the program - at the first run it sets up the streaming context
    * with the overridden processing logic. On restart, it recovers the streaming
    * context from the checkpoint
    *
    * @param args command line arguments
    */
  final def main(args: Array[String]): Unit = {
    val ssc = StreamingContext.getOrCreate(
      getCheckPointPath,
      () => createStreamingContext(streamConfig.parse(args)))

    val err = new StreamingErrorHandler
    ssc.addStreamingListener(err)
    monitorApplication(err)

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * Spins up another thread that monitors for an abort flag in the streaming
    * listener instance. The listener callbacks as well as the monitor
    * application thread are executed on the driver, and hence a call to abort()
    * from here will result in the application being shut down
    *
    * @param err Streaming listener instance
    */
  final def monitorApplication(err: StreamingErrorHandler): Unit = {
    new Thread("monitor-application") {
      override def run(): Unit = {
        while(!err.shouldAbort) {
          Thread.sleep(batchDurationSeconds * 1000)
        }
        newLogger().error("Aborting application due to error")
        abort()
      }
    }.start()
  }

  /**
    * Stop this streaming application
    *
    * Useful in cases where it is desirable to halt the streaming application after
    * all the retries for an operation are exhausted. This can prevent the receiver
    * from queuing up message in the WAL when the output operations are not working.
    * This call needs to be made in the driver JVM, else it will have no effect.
    *
    * An external monitor can keep probing the job for status and attempt a limited
    * number of restarts before creating a monitoring alert.
    *
    * NOTE: currently there is no support to exit with a non-zero status code. External
    * systems must be able to differentiate job abort from graceful stop during releases
    */
  final def abort(): Unit = {
    StreamingContext
      .getActive()
      .fold(
        // This should never happen, abort is called from a running streaming
        // application which would ensure that streaming context is available.
        // But probably worth setting up a Kibana/Stackdriver logging alert
        // for the string "UnhandledFailure"
        newLogger().error("UnhandledFailure: No active streaming context found")
      )(_.stop())
  }
}

class StreamingErrorHandler extends StreamingListener {
  @volatile private var shouldAbortFlag: Boolean = false
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /** Called when a receiver has reported an error */
  override def onReceiverError(receiverError: StreamingListenerReceiverError): Unit = {
    logger.error(s"Receiver error: ${receiverError.receiverInfo.lastErrorMessage}")
    this.shouldAbortFlag = true
  }

  /** Called when processing of a batch of jobs has completed. */
  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    batchCompleted
      .batchInfo
      .outputOperationInfos
      .values
      .foreach { ooi =>
        if (ooi.failureReason.nonEmpty) {
          logger.error(s"Batch failed with error: ${ooi.failureReason.get}")
          this.shouldAbortFlag = true
        }
      }
  }

  def shouldAbort: Boolean = shouldAbortFlag
}
