package com.example.prodspark.client

import java.io.IOException

import com.example.prodspark.util.RetryUtil
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.{Bigquery, BigqueryScopes}
import com.google.api.services.bigquery.model._
import com.google.cloud.hadoop.io.bigquery.BigQueryStrings
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

object BigQueryClient {
  private val SCOPES = List(BigqueryScopes.BIGQUERY).asJava

  private val bigquery: Bigquery = {
    val credential = GoogleCredential.getApplicationDefault.createScoped(SCOPES)
    new Bigquery.Builder(new NetHttpTransport, new JacksonFactory, credential)
      .setApplicationName("spark-bigquery")
      .build()
  }

  /**
    * Streaming insert API
    * @param table BigQuery table specification
    * @param req Streaming insert request
    * @return
    */
  @throws[java.io.IOException]
  private def insertAll(table: TableReference, req: TableDataInsertAllRequest): TableDataInsertAllResponse = {
    bigquery.tabledata().insertAll(
      table.getProjectId,
      table.getDatasetId,
      table.getTableId,
      req
    ).execute()
  }

  /**
    * Streaming insert into BigQuery Table
    * @param rows List of TableDataInsertAllRequest.Rows
    * @param tableSpec Fully qualified BQ table name
    * @param projectId projectID used to get the right BQ client object
    */
  def streamingInsert(
    rows: Seq[TableDataInsertAllRequest.Rows],
    tableSpec: String,
    projectId: String
  ): Unit = {
    lazy val tableRef = BigQueryStrings.parseTableReference(tableSpec)

    val req = new TableDataInsertAllRequest()
      .setRows(rows.asJava)
      .setIgnoreUnknownValues(true)

    RetryUtil.retry(3) {
      val response = insertAll(tableRef, req)
      if (response.getInsertErrors != null) {
        log.error("Error inserting into BigQuery: " + response.getInsertErrors.toString)
        throw new IOException(response.getInsertErrors.toString)
      }
    }
  }

  private val log = LoggerFactory.getLogger(this.getClass)
}
