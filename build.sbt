name := "spark-streaming-gcp"

version := "0.1"

scalaVersion := "2.11.8"

resolvers += "Sonatype OSS Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/"

// for debugging sbt problems
logLevel := Level.Warn

val sparkVersion = "2.0.2"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"  % sparkVersion,
  "org.apache.spark" %% "spark-streaming"  % sparkVersion,

  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.slf4j" % "slf4j-api" % "1.7.21",

  "com.google.cloud.bigdataoss" % "bigquery-connector" % "0.10.0-hadoop2",
  "com.google.apis" % "google-api-services-pubsub" % "v1-rev11-1.22.0"
    exclude ("com.google.guava", "guava-jdk5")
)