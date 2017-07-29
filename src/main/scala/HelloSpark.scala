import System.out

import org.apache.spark.{SparkConf, SparkContext}


object HelloSpark {
  def main(args: Array[String]): Unit = {

    out.println("hi Spark")
    val conf = new SparkConf()
    conf.setAppName("Hello Spark")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    out.println(sc)

  }
}