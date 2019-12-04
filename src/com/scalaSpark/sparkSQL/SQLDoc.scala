package com.scalaSpark.sparkSQL

import org.apache.spark.sql._
import org.apache.spark.SparkContext
import java.io.File

case class student1(SID: Int, Name: String, Age: Int, Gender: String)

object SQLDoc {
  def main(args: Array[String]): Unit = {

    println("\n starting point: SparkSession - Create Spark DataFrames from an existing RDD")
    val warehouse = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession.builder().appName("SparkSQL basic Example").master("local").config("spark.sql.warehousee.dir", warehouse).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    println("\nloading a json file and convert into DF")
    val loadJson = spark.read.json("/Users/vinodkumarvadlamudi/data-master/retail_db_json/order_items/part-00002.json")
    val jsonToDF = loadJson.toDF()
    val twoJson = jsonToDF.select("order_item_id", "order_item_subtotal")

    println("\nPerform operations on a DataFrame")
    val filtering = twoJson.filter(twoJson.col("order_item_subtotal") < 50)
    twoJson.createOrReplaceTempView("Order_items")
    val OrderItemsTab = spark.sql("select * from Order_items")
    OrderItemsTab.show()
    filtering.createGlobalTempView("filteringTest")
    spark.sql("select * from global_temp.filteringTest").show()

    println("\nWrite a Spark SQL application that reads and writes data from Hive tables")
    import spark.implicits._
    import spark.sql
    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    val hiveTable = sql("LOAD DATA LOCAL INPATH '/Users/vinodkumarvadlamudi/Downloads/spark-2.4.3-bin-hadoop2.7/examples/src/main/resources/kv1.txt' INTO TABLE src")
    //sql("SELECT * FROM src").show()
    //sql("SELECT * FROM SRC WHERE key < 10 ORDER BY key").show()
    sql("SELECT COUNT (*) FROM src").show()
    val convertHiveDF = hiveTable.toDF()

  }
}