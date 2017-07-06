package com.thumbtack.common.metrics

import com.thumbtack.common.model.DeploymentEnvironment
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

trait SparkStreamingMonitor extends GenericMonitor {
  /**
    * Uses an action on the RDD to mark a meter on number of items.
    * Suitable in streaming applications within dstream.foreachRDD, which
    * is already an output action
    *
    * DO NOT USE FOR BATCH operations with large dataset in each partition
    *
    * @see [[SparkMonitor.lazyCountRDD()]]
    * @param rdd RDD to be counted
    * @param metricName Name of the metric
    * @param env Deployment environment
    * @tparam U Type of RDD items
    */
  def meterRDD[U: ClassTag](
    rdd: RDD[U],
    metricName: String,
    env: DeploymentEnvironment = DeploymentEnvironment.Dev
  ): Unit = {
    meter(metricName, env).mark(rdd.count())
  }

  /**
    * Uses an action on the RDD to add the count of number of items.
    * Suitable in streaming applications within dstream.foreachRDD, which
    * is already an output action
    *
    * DO NOT USE FOR BATCH operations with large dataset in each partition
    *
    * @see [[SparkMonitor.lazyCountRDD()]]
    * @param rdd RDD to be counted
    * @param metricName Name of the metric
    * @param env Deployment environment
    * @tparam U Type of RDD items
    */
  def countRDD[U: ClassTag](
    rdd: RDD[U],
    metricName: String,
    env: DeploymentEnvironment = DeploymentEnvironment.Dev
  ): Unit = {
    counter(metricName, env).inc(rdd.count())
  }
}
