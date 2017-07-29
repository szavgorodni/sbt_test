import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SparkSession}


// https://github.com/abbas-taher/the-7-ways-wordcount-apache-spark-snippets
// command line params:
// menu:Run:EditConfigurations:Program Arguments
// local[*] E:\projects\spark\s\data\humpty.txt

object WordCount {
  var sc: SparkContext = null
  //var ss: SparkSession = null
  var spark_master: String = null

  // used to apply the data types to the data set
  // has to be defined on object/class level - otherwise compiler errors out at compile
  case class CWord(Value: String)

  def main(args: Array[String]): Unit = {

    spark_master = args(0)
    val inputFile = args(1)

    println(spark_master)
    println(inputFile)

    // disable excessive logging
    import org.apache.log4j.Logger
    import org.apache.log4j.Level
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf()
    conf.setAppName("Word Count")
    conf.setMaster(spark_master)
    sc = new SparkContext(conf)

    // disable excessive logging
    sc.setLogLevel("WARN")

    // :: Classic Word Count using filter & reduceByKey on RDD
    wc_rdd(inputFile)
    // :: Example 4: Word Count Using Dataset
    wc_ds(inputFile)
    // :: Example 5: Word Count Using Spark SQL on Dataset & TempView
    wc_ds_sql(inputFile)
    // :: Example 6: Word Count Using Case Class, Dataset and Value Attribute
    wc_ds_sql_case(inputFile)
    // :: Example 7: Word Count Using Case Class, Dataset, agg operation and $column-name
    wc_ds_sql_case_col_name(inputFile)

  }

  def wc_rdd(inputFile:String): Unit = {
    println(":: Example 1: Classic Word Count using filter & reduceByKey on RDD")

    val readFileRDD = sc.textFile(inputFile)
    val wcounts1 = readFileRDD.flatMap(line=>line.split(" "))
      .filter(w => (w =="Humpty") || (w == "Dumpty"))
      .map(word=>(word, 1))
      .reduceByKey(_ + _)
    wcounts1.collect.foreach(println)
  }

  def wc_ds(inputFile:String): Unit = {
    println(":: Example 4: Word Count Using Dataset")

    val ss = SparkSession.builder
      .master(spark_master)
      .appName("Word_Count_Example")
      .getOrCreate()

    // provides object encoder/serializer  for primitive data types
    // NB:! imported from object - not class
    import ss.implicits._


    val dfsFilename = inputFile
    val readFileDS = ss.read.textFile(dfsFilename)
    val wcounts4 = readFileDS.flatMap(_.split(" "))
      .filter(w => (w =="Humpty") || (w == "Dumpty"))
      .groupBy("Value")
      .count()
    wcounts4.show()

    // +------+-----+
    // | Value|count|
    // +------+-----+
    // |Dumpty|    2|
    // |Humpty|    3|
    // +------+-----+
  }


  def wc_ds_sql(inputFile:String): Unit = {
    println(":: Example 5: Word Count Using Spark SQL on Dataset & TempView")

    val ss = SparkSession.builder
      .master(spark_master)
      .appName("Word_Count_Example")
      .getOrCreate()

    // provides object encoder/serializer  for primitive data types
    // NB:! imported from object - not class
    import ss.implicits._

    val readFileDS = ss.read.textFile(inputFile)
    val wordsDS = readFileDS.flatMap(_.split(" ")).as[String]
    wordsDS.createOrReplaceTempView("WORDS")
    val wcounts5 = ss.sql(
      "SELECT Value, COUNT(Value) " +
      "FROM WORDS " +
      "WHERE Value ='Humpty' OR Value ='Dumpty' " +
      "GROUP BY Value"
    )
    wcounts5.show()

    // +------+------------+
    // | Value|count(Value)|
    // +------+------------+
    // |Dumpty|           2|
    // |Humpty|           3|
    // +------+------------+
  }


  def wc_ds_sql_case(inputFile:String): Unit = {
    println(":: Example 6: Word Count Using Case Class, Dataset and Value Attribute")

    val ss = SparkSession.builder
      .master(spark_master)
      .appName("Word_Count_Example")
      .getOrCreate()

    // provides object encoder/serializer  for primitive data types
    // NB:! imported from object - not class
    import ss.implicits._

    val readFileDS = ss.sqlContext
        .read
        .textFile(inputFile)
        .flatMap(_.split(" "))
        .as[CWord]

   val wcounts6 = readFileDS.filter (w => (w.Value == "Humpty") || (w.Value == "Dumpty"))
       .groupBy("Value")
       .count()
   wcounts6.collect.foreach(println)

    // [Dumpty,2]
    // [Humpty,3]
  }

  def wc_ds_sql_case_col_name(inputFile:String): Unit = {
    println(":: Example 7: Word Count Using Case Class, Dataset, agg operation and $column-name")

    val ss = SparkSession.builder
      .master(spark_master)
      .appName("Word_Count_Example")
      .getOrCreate()

    // provides object encoder/serializer  for primitive data types
    // NB:! imported from object - not class
    import ss.implicits._

    val readFileDS = ss.sqlContext
      .read
      .textFile(inputFile)
      .flatMap(_.split(" "))
      .as[CWord]

    val wcounts7 = readFileDS
      .where( ($"Value" === "Humpty") || ($"Value" === "Dumpty") )
      .groupBy($"Value")
      .count()
    wcounts7.collect.foreach(println)

    // [Dumpty,2]
    // [Humpty,3]
  }
}

