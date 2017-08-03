name := "sbt_test"

version := "1.0"

scalaVersion := "2.11.11"

val sparkVersion = "2.2.2"

resolvers ++= Seq(

  "apache-snapshots" at "http://repository.apache.org/snapshots/"

)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion exclude("jline", "2.12"),
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.hadoop" % "hadoop-common" % "2.7.0" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy"),
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.spark" %% "spark-yarn" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-graphx" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion"
)