package com.thumbtack.common.metrics

import java.net.InetAddress
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import com.codahale.metrics.{MetricRegistry, ScheduledReporter}
import com.google.common.collect.{ImmutableMap, ImmutableSet}
import com.izettle.metrics.dw.InfluxDbReporterFactory
import com.thumbtack.common.model.DeploymentEnvironment
import com.thumbtack.common.sources.CredentialsSource
import com.thumbtack.common.util.RetryUtil

import scala.collection.JavaConversions._

private[metrics] case class InfluxMetricsReporter(
  env: DeploymentEnvironment,
  cfg: InfluxMetricsConfig
) extends CredentialsSource {

  private[metrics] val registry = new MetricRegistry()

  private val reporter: ScheduledReporter = init(cfg, env)

  private def builder(
    cfg: InfluxMetricsConfig,
    env: DeploymentEnvironment
  ): InfluxDbReporterFactory = {
    val c = cfg.cloudProvider match {
      case "GCP" => getCredentials(s"influxdb_${cfg.dbName}")
      case "AWS" => getCredentialsAws(cfg.dbName)

      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
    val tags = cfg.tags ++ Map(
      "environment" -> env.name,
      // use hostname as tag so that influx does not treat
      // metrics from different executors as duplicate
      "hostname" -> InetAddress.getLocalHost.getHostName
    )

    val builder = new InfluxDbReporterFactory
    builder.setProtocol("https")
    builder.setHost(cfg.dbServiceHostName)
    builder.setPort(cfg.dbServicePort)
    builder.setAuth(s"${c.username}:${c.password}")
    builder.setDatabase(cfg.dbName)
    builder.setTags(mapAsJavaMap(tags))
    builder.setFields(ImmutableMap.of(
      "timers", ImmutableSet.of("count", "min", "max", "mean", "p50", "p99", "m1_rate"),
      "meters", ImmutableSet.of("count", "m1_rate"))
    )
    builder
  }

  private def init(
    cfg: InfluxMetricsConfig,
    env: DeploymentEnvironment
  ): ScheduledReporter = {
    val reporter = RetryUtil.retry(3) {
      builder(cfg, env).build(registry)
    }
    if (cfg.periodicReporting) {
      reporter.start(cfg.reportingIntervalInMinutes, TimeUnit.MINUTES)
    }
    reporter
  }

  /**
    * Force an immediate flush of metrics to the server.
    */
  def report(): Unit = reporter.report()
}

object InfluxMetricsReporter {
  private type Config = (DeploymentEnvironment, InfluxMetricsConfig)

  private val reporters = new ConcurrentHashMap[Config, InfluxMetricsReporter](1, 1, 1)

  private val createReporter = new java.util.function.Function[Config, InfluxMetricsReporter] {
    override def apply(
      input: (DeploymentEnvironment, InfluxMetricsConfig)
    ): InfluxMetricsReporter = new InfluxMetricsReporter(input._1, input._2)
  }

  def get(env: DeploymentEnvironment, cfg: InfluxMetricsConfig): InfluxMetricsReporter = {
    val key = (env, cfg)
    reporters.computeIfAbsent(key, createReporter)
  }
}

case class InfluxMetricsConfig(
  /** Name of the database provisioned on influxdb server */
  dbName: String,

  /** Arbitrary application specific tags to be added. Env is automatically added as a tag */
  tags: Map[String, String] = Map.empty[String, String],

  /** Runtime cloud environment. Credential access methods differ for each cloud */
  cloudProvider: String = "GCP",

  /** A reporter thread is started on each JVM if true */
  periodicReporting: Boolean = true,

  /** Periodicity of reports for scheduled reporting */
  reportingIntervalInMinutes: Int = 1,

  /** InfluxDB server hostname/CNAME */
  dbServiceHostName: String = "production.influxdb.thumbtack.com",

  /** InfluxDB server port */
  dbServicePort: Int = 8086
)