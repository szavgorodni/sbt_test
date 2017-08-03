import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SparkSession}
import java.sql.Date

import java.io.File
import org.apache.commons.io._

// refer to data types here:
//https://gist.github.com/yoyama/ce83f688717719fc8ca145c3b3ff43fd
// https://spark.apache.org/docs/latest/sql-programming-guide.html#dataframegroupby-retains-grouping-columns
object DSScore {

  // name of class attributes have to be exactly same as as header in the file
  // otherwise the file will not be parsed
  // NB: import java.sql.Date for correct date type parsing
  case class SStore(
    row_id:Int,
    order_id:String,
    order_date:Date,
    ship_date:Date,
    ship_mode:String,
    customer_id:String,
    customer_name:String,
    segment:String,
    postal_code:String,
    city:String,
    state:String,
    country:String,
    region:String,
    market:String,
    product_id:String,
    category:String,
    sub_category:String,
    product_name:String,
    sales:Double,
    quantity:Int,
    discount:Double,
    profit:Double,
    shipping_cost:Double,
    order_priority:String
  )

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

    val ss :SparkSession = SparkSession.builder
      .master(spark_master)
      .enableHiveSupport() // hive dependency added to build.sbt
      // already created in E:\projects\spark\s\sbt_test\spark-warehouse
      //.config("spark.sql.warehouse.dir", "E:\\projects\\spark\\s\\sbt_test\\target\\spark-warehouse")
      .appName(app_name)
      .getOrCreate()

    // provides object encoder/serializer  for primitive data types
    // NB:! imported from object - not class
    import ss.implicits._


    // https://github.com/databricks/spark-csv
    // A library for parsing and querying CSV data with Apache Spark, for Spark SQL and DataFrames.
    // This functionality has been inlined in Apache Spark 2.x.
    // date formats descibed here:
    // https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html

    // data frame
    val df_score = ss
      .read
      // use .format() if .load(my_file) is used : i.e. format is not specified
      // and .load(inputFile) is used instead of csv()
      //.format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("delimiter", "\t")
      .option("dateFormat","MM/dd/yyyy")
      .option("inferSchema","true") // makes spark to make two passes on data: TODO: check how to avoid it
      .csv(inputFile)
    // .load(inputFile) //generic loader // have to specify format in that case

    // data set
    val ds_score = ss
      .read
      // use .format() if .load(my_file) is used : i.e. format is not specified
      // and .load(inputFile) is used instead of csv()
      //.format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("delimiter", "\t")
      .option("dateFormat","MM/dd/yyyy")
      .option("inferSchema","true") // makes spark to make two passes on data: TODO: check how to avoid it
      .csv(inputFile)
      // .load(inputFile) //generic loader // have to specify format in that case
      .as[SStore]

    // referring to ds is different than referring to df
    val selectedDF = df_score.select("order_id")

    // more object oriented way to query the data set
    val selectedDS = ds_score.map(_.order_id)

    // output execution plan
    // shows that data set is mapped to the data types and data frame is not typed
    println(selectedDF.queryExecution.optimizedPlan.numberedTreeString)
    println(selectedDS.queryExecution.optimizedPlan.numberedTreeString)

    val  ds_score_count = ds_score.count()

    // just output the sample in tab format
    ds_score.show(10)

    println(ds_score_count)

    val ds_score_vw = ds_score.createOrReplaceTempView("ds_score_vw")
    val hql_country ="'United States'"
    val ds_score_group = ss.sql(
      s"""
         |SELECT
         |   t.state
         | , t.city
         | , t.orders
         | , (1 / (1 + exp(-1 * t.orders)))*2-1 as squash_score_orders -- 	(1 / (1 + exp(-x)))*2-1; alternative: tanh
         | , tanh(t.orders) as tanh_score_orders -- alternative: tanh
         | , MAX(t.orders) OVER() as max_orders
         | , FLOOR((t.orders / MAX(t.orders) OVER())*10) as score_orders
         | from (
         |SELECT
         |  state
         | , city
         | , sum(quantity) as orders
         |FROM ds_score_vw
         |WHERE country = $hql_country
         |GROUP BY state, city
         | ) t
         |ORDER BY t.state, t.city
      """.stripMargin // do not use () -- not sure why
    )


    val output_directory = "E:\\projects\\spark\\s\\sbt_test\\output\\ds_score_group"
    FileUtils.deleteDirectory(new File(output_directory))

    // save output
    ds_score_group
      .repartition(1) // save everything to one file
      .write
      .format("com.databricks.spark.csv")
      .option("header","true")
      .option("delimiter", "\t")
      .save(output_directory)

    ds_score_group.show(10)

    // temp tables created for work with spark SQL
    ss.catalog.listTables.show()

    ss.stop()

  }
}
