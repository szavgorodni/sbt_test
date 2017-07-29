import WordCount.{sc, spark_master}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SparkSession}


// command line params:
// menu:Run:EditConfigurations:Program Arguments
// local[*] E:\projects\data\global_superstore_2016.txt

object DSScore {

  var sc: SparkContext = null
  //var ss: SparkSession = null
  var spark_master: String = null

  def main(args: Array[String]): Unit = {

    // uncomment for prod run
    // spark_master = args(0)
    // val inputFile = args(1)

    // just for debug purpoces -- use the construct above for prod
    spark_master = "local[*]"
    val inputFile = "E:\\projects\\data\\global_superstore_2016.txt"

    println(spark_master)
    println(inputFile)

    // disable excessive logging
    import org.apache.log4j.Logger
    import org.apache.log4j.Level
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val app_name = "product score"

    // define the main spark entry points
    val conf = new SparkConf()
    conf.setAppName(app_name)
    conf.setMaster(spark_master)
    // object level: used by many object methods
    sc = new SparkContext(conf)

    // disable excessive logging
    sc.setLogLevel("WARN")

    val ss = SparkSession.builder
      .master(spark_master)
      .appName(app_name)
      .getOrCreate()

    // https://github.com/databricks/spark-csv
    // A library for parsing and querying CSV data with Apache Spark, for Spark SQL and DataFrames.
    // This functionality has been inlined in Apache Spark 2.x.
    // date formats descibed here:
    // https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html

    val ds_score = ss
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("delimiter", "\t")
      .option("dateFormat","MM/dd/yyyy")
      .option("inferSchema","true")
      .load(inputFile)

    val  ds_score_count = ds_score.count()
    ds_score.show()
    println(ds_score_count)
   // ds_score_count.show()

  }
}
