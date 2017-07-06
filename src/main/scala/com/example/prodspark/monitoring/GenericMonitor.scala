package com.thumbtack.common.metrics

import com.codahale.metrics._
import com.thumbtack.common.model.DeploymentEnvironment

import scala.reflect.ClassTag

/**
  * This trait exposes different types of metrics (meter, counter, gauge etc) tied
  * to a InfluxDB Reporter. If periodic reporting is enabled, each metric registration
  * attempts to start a reporter thread - which actually starts a thread in the first
  * attempt on each JVM (i.e. in a Spark application a reporter thread is started on each
  * worker as well as the driver)
  *
  * @see [[SparkMonitor]] for RDD metrics in batch jobs
  * @see [[SparkStreamingMonitor]] for materialized micro-batch RDD metrics
  */
trait GenericMonitor {
  val HistogramPrefix: String = "histogram."
  val TimerPrefix: String = "time."
  val MeterPrefix: String = "meter."
  val CounterPrefix: String = "count."
  val GaugePrefix: String = "gauge."

  def influxCfg: InfluxMetricsConfig = InfluxMetricsConfig(dbName = "default")

  protected def withClassName(metricName: String): String = {
    val className = getClass.getSimpleName
    val cleanClassName = if(className.endsWith("$"))  className.dropRight(1) else className
    cleanClassName + "." + metricName
  }

  /**
    * Accessor for a meter on the underlying metrics registry
    * Starts the reporter thread when called for the first time
    * within an executor (or any JVM)
    * @param metricName Name of the metric
    * @param env Deployment environment
    * @return Meter metric holder
    */
  def meter(
    metricName: String,
    env: DeploymentEnvironment = DeploymentEnvironment.Dev
  ): Meter = {
    val name = MeterPrefix + withClassName(metricName)
    InfluxMetricsReporter.get(env, influxCfg).registry.meter(name)
  }

  /**
    * Accessor for a counter on the underlying metrics registry
    * Starts the reporter thread when called for the first time
    * within an executor (or any JVM)
    * @param metricName Name of the metric
    * @param env Deployment environment
    * @return Counter metric holder
    */
  def counter(
    metricName: String,
    env: DeploymentEnvironment = DeploymentEnvironment.Dev
  ): Counter = {
    val name = CounterPrefix + withClassName(metricName)
    InfluxMetricsReporter.get(env, influxCfg).registry.counter(name)
  }

  /**
    * Accessor for a histogram on the underlying metrics registry
    * Starts the reporter thread when called for the first time
    * within an executor (or any JVM)
    * @param metricName Name of the metric
    * @param env Deployment environment
    * @return Histogram metric holder
    */
  def histogram(
    metricName: String,
    env: DeploymentEnvironment = DeploymentEnvironment.Dev
  ): Histogram = {
    val name = HistogramPrefix + withClassName(metricName)
    InfluxMetricsReporter.get(env, influxCfg).registry.histogram(name)
  }

  /**
    * Accessor for a timer on the underlying metrics registry
    * Starts the reporter thread when called for the first time
    * within an executor (or any JVM)
    * @param metricName Name of the metric
    * @param env Deployment environment
    * @return Timer metric holder
    */
  def timer(
    metricName: String,
    env: DeploymentEnvironment = DeploymentEnvironment.Dev
  ): Timer = {
    val name = TimerPrefix + withClassName(metricName)
    InfluxMetricsReporter.get(env, influxCfg).registry.timer(name)
  }

  /**
    * Accessor for a gauge on the underlying metrics registry
    * Starts the reporter thread when called for the first time
    * within an executor (or any JVM)
    * @param metricName Name of the metric
    * @param value Instantaneous value to be recorded
    * @param env Deployment environment
    * @tparam U Type of the instantaneous value
    */
  def gauge[U: ClassTag](
    metricName: String,
    value: U,
    env: DeploymentEnvironment = DeploymentEnvironment.Dev
  ): Unit = {
    val name = GaugePrefix + withClassName(metricName)
    InfluxMetricsReporter.get(env, influxCfg).registry.register(name, new Gauge[U] {
      override def getValue: U = value
    })
  }

  /**
    * Mark a meter on each item in a DStream or RDD transformation.
    * Usage: dstream.map(meterItem(_, "metricName")
    *
    * @param item an item within a RDD or DStream
    * @param metricName Name of the metric
    * @param env Deployment environment
    * @tparam U Type of the item
    * @return Same item is return for downstream transformations
    */
  def meterItem[U: ClassTag](
    item: U,
    metricName: String,
    env: DeploymentEnvironment = DeploymentEnvironment.Dev
  ): U = {
    meter(metricName, env).mark()
    item
  }

  /**
    * Increment counter on each item in a DStream or RDD transformation.
    * Usage: dstream.map(meterItem(_, "metricName")
    *
    * @param item an item within a RDD or DStream
    * @param metricName Name of the metric
    * @param env Deployment environment
    * @tparam U Type of the item
    * @return Same item is return for downstream transformations
    */
  def countItem[U: ClassTag](
    item: U,
    metricName: String,
    env: DeploymentEnvironment = DeploymentEnvironment.Dev
  ): U = {
    counter(metricName, env).inc()
    item
  }

  /**
    * Provides the ability to instrument the time a computation
    * takes and save to influx. A typical use of this would be to measure
    * the time taken to write to a database with the caveat that this
    * function needs to be applied to an action instead of a transformation.
    *
    * @param metricName  Name of the metric
    * @param env Deployment environment
    * @param computation The computation that need to be measured
    * @return the resulting value of the computation
    */
  def timedComputation[A](
    metricName: String,
    env: DeploymentEnvironment = DeploymentEnvironment.Dev
  )(
    computation: => A
  ) : A = {
    val computationTimer = timer(metricName, env).time()
    try {
      computation
    } finally {
      computationTimer.stop()
      InfluxMetricsReporter.get(env, influxCfg).report()
    }
  }

  /**
    * Override the periodic reporter to send metrics immediately
    * This is useful in cases where an error needs to be captured before the
    * JVM is aborted
    *
    * @param env Deployment environment
    */
  def reportNow(
    env: DeploymentEnvironment = DeploymentEnvironment.Dev
  ): Unit = InfluxMetricsReporter.get(env, influxCfg).report()
}
