name := "sbt_test"

version := "1.0"

scalaVersion := "2.11.11"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.0",
  "org.apache.spark" %% "spark-sql" % "2.2.0",
  "org.apache.hadoop" % "hadoop-common" % "2.7.0" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy"),
  "org.apache.spark" %% "spark-hive" % "2.2.0" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy"),
  "org.apache.spark" %% "spark-yarn" % "2.2.0" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy"),
  "org.apache.spark" %% "spark-mllib" % "2.2.0",
  "org.apache.spark" %% "spark-streaming" % "2.2.0",
  "org.apache.spark" %% "spark-graphx" % "2.2.0",
  "org.apache.spark" %% "spark-core" % "2.2.0"
)