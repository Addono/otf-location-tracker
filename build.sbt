name := "sparkstreaming-location-tracker"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.5",
  "org.apache.spark" %% "spark-streaming" % "2.4.5",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.5",
  "org.datasyslab" % "geospark" % "1.3.1",
)
