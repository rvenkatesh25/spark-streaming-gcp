package com.example.prodspark.connector

import java.nio.charset.StandardCharsets
import java.util

import com.example.prodspark.client.BigQueryClient
import com.google.api.services.bigquery.model.TableDataInsertAllRequest
import com.google.gson.Gson
import com.example.prodspark.util.ListUtil._
import org.apache.spark.FutureAction
import org.apache.spark.rdd.{AsyncRDDActions, RDD}
import org.slf4j.LoggerFactory

object BigQuerySink {
  lazy val targetClass: Class[_ <: util.HashMap[String, Object]] =
    new java.util.HashMap[String, Object].getClass

  lazy val gson = new Gson

  /**
    * Return size of a string in bytes, using utf-8 encoding
    * @param s input string
    * @return size in bytes
    */
  private[connector] def stringSize(s: String) = s.getBytes(StandardCharsets.UTF_8).length

  /**
    * Filter method to enforce if a given json string is smaller
    * than the limits enforced by BigQuery Streaming Inserts
    * @param r input string, usually json
    * @param limits BigQuery streaming insert limits, override-able for tests
    * @return whether it's within limits or not
    */
  private[connector] def withinRowLimit(
    r: String
  )(
    implicit limits: BigQueryInsertLimits = BigQueryInsertLimits.default
  ): Boolean = {
    if (stringSize(r) > limits.ROW_LIMIT_BYTES) {
      log.warn(s"[Quota Warning] Found a row bigger than" +
       s"${limits.ROW_LIMIT_BYTES} bytes: $r")
      false
    } else {
      true
    }
  }

  /**
    * Convert a given json string and id into a BigQuery Row object
    * @param dataWithId rdd of (id, jsonRow) tuple
    * @return rdd of tuple (size, BigQueryTableRow)
    */
  private[connector] def toBigQueryRowWithSize(
    dataWithId: RDD[(String, String)]
  ): RDD[(Int, TableDataInsertAllRequest.Rows)] = {
    dataWithId
      .filter(r => withinRowLimit(r._2))
      .map { dwi =>
        val gsonRow = gson.fromJson(dwi._2, targetClass)
        (
          stringSize(dwi._2),
          new TableDataInsertAllRequest.Rows()
            .setJson(gsonRow)
            .setInsertId(dwi._1)
        )
      }
  }

  /**
    * Split the given list of rows into smaller quota-bound lists if
    * necessary, and call the streaming insert method on bq client
    * @param rows tuple (size, BigQueryTableRow)
    * @param tableSpec Fully qualified BQ table name
    * @param projectId project id
    * @param limits BigQuery streaming insert limits, override-able for tests
    */
  private[connector] def bqInsert(
    rows: List[(Int, TableDataInsertAllRequest.Rows)],
    tableSpec: String,
    projectId: String
  )(
    implicit limits: BigQueryInsertLimits = BigQueryInsertLimits.default
  ): Unit = {
    rows
      .groupedBySize(limits.REQUEST_LIMIT_BYTES)
      .foreach { c =>
        BigQueryClient.streamingInsert(c.map(_._2), tableSpec, projectId)
        log.debug(s"[$tableSpec] Inserted ${c.length} rows")
      }
  }

  /**
    * Streaming insert into BigQuery table with quota enforcement
    *
    * - Filters out rows that are bigger than a single row size limit of 1MB
    *
    * - Within each partition, the list of rows are further broken down into
    *   multiple requests such that each request is smaller than the HTTP
    *   request size limit.
    *
    *   The limit is actually 10 MB, but since we cannot find out the exact
    *   size of the actual HTTP request object, we enforce that the size of
    *   the payload be less than 7MB, and use the remaining 3MB as reserve
    *   space for rest of the metadata and encoding
    *
    * - The other limits are 100,000 msg/s or 100 MB/s insert rate on a
    *   single table. Currently these limits are not enforced
    *
    * @param dataWithId RDD of tuple (id, data)
    * @param tableSpec Fully qualified BQ table name
    * @param projectId project id
    */
  def insert(
    dataWithId: RDD[(String, String)],
    tableSpec: String,
    projectId: String
  ): Unit = {
    toBigQueryRowWithSize(dataWithId)
      .foreachPartition { r =>
        if (r.nonEmpty) {
          val rows = r.toList
          bqInsert(rows, tableSpec, projectId)
        }
      }
  }

  /**
    * If it is required to insert rows into multiple BigQuery tables for each
    * micro-batch, it might be necessary to do them in parallel else the processing
    * time may get bigger than the batch interval
    *
    * Spark engine, by default, processes only one RDD at a time with massive
    * parallelism, but here we need multiple RDDs leading into their own output
    * operations to be executed in parallel. For such use cases, this async version
    * of streaming insert should be used (which uses [[AsyncRDDActions.foreachPartitionAsync]])
    *
    * NOTE: spark.scheduler.mode should be changed to FAIR (from the default FIFO)
    * in spark-defaults.conf at cluster creation
    *
    * @param dataWithId RDD of tuple (id, data)
    * @param tableSpec Fully qualified BQ table name
    * @param projectId project id
    * @return a future object representing the completion of insert
    */
  def insertAsync(
    dataWithId: RDD[(String, String)],
    tableSpec: String,
    projectId: String
  ): FutureAction[Unit] = {
    toBigQueryRowWithSize(dataWithId)
      .foreachPartitionAsync { r =>
        if (r.nonEmpty) {
          val rows = r.toList
          bqInsert(rows, tableSpec, projectId)
        }
      }
  }

  private val log = LoggerFactory.getLogger(this.getClass)
}

case class BigQueryInsertLimits(
  ROW_LIMIT_BYTES: Int,
  REQUEST_LIMIT_BYTES: Int
)

object BigQueryInsertLimits {
  val default = BigQueryInsertLimits(
    // 1MB max single row size
    ROW_LIMIT_BYTES = 1024 * 1024,

    // 10MB max HTTP request size. Reserve 3MB for other fields in
    // the request object and account for encoding etc (base64 alone
    // results in a 33% increase in size)
    // Use 7MB as payload size limit
    REQUEST_LIMIT_BYTES = 7 * 1024 * 1024
  )
}

