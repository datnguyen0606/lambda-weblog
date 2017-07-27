package batch

import java.lang.management.ManagementFactory

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}
import domain._

object BatchJob {
  def main(args: Array[String]): Unit = {

    // get spark configuration
    val conf = new SparkConf()
      .setAppName("Lambda with Spark")

    // Check if running from IDE
    if (ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")) {
      conf.setMaster("local[*]")
    }

    // setup spark context
    val sc = new  SparkContext(conf)

    // initialize input RDD
    val sourceFile = "file:///Users/datnguyen/Documents/Working/spark-lambda-architecture/vagrant/data.tsv"
    val input = sc.textFile(sourceFile)

    val inputRDD = input.flatMap{ line =>
      val record = line.split("\\t")
      val MS_IN_HOUR = 1000 * 60 * 60
      if (record.length == 7)
        Some(Activity(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR, record(1), record(2), record(3), record(4), record(5), record(6)))
      else
        None
    }

    val keyedByProduct = inputRDD.keyBy(a => (a.product, a.timestamp_hour)).cache()
    val visitorByProduct = keyedByProduct
      .mapValues(a => a.visitor)
      .distinct()
      .countByKey()

    val activityByProduct = keyedByProduct
      .mapValues(a =>
        a.action match {
          case "purchase" => (1, 0, 0)
          case "add_to_cart" => (0, 1, 0)
          case "page_view" => (0, 0, 1)
        }
      ).reduceByKey( (a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))


    visitorByProduct.foreach(println)
    activityByProduct.foreach(println)
  }
}
