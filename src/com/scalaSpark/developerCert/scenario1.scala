package com.scalaSpark.developerCert

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
object scenario1 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("scenario1").master("local").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    val loadRdd = spark.read.json("/Users/vinodkumarvadlamudi/data-master/retail_db_json/order_items/part-r-00000-6b83977e-3f20-404b-9b5f-29376ab1419e").toDF()
    val filtering = loadRdd.filter($"order_item_product_price" < 200)
    val getCol = filtering.select("order_item_quantity", "order_item_product_price")
    val finalResult = getCol.groupBy("order_item_quantity").avg("order_item_product_price").orderBy(col("avg(order_item_product_price)").desc)
    finalResult.repartition(1).write.csv("/Users/vinodkumarvadlamudi/eclipse/Interviews/HDPCSparkDeveloper/src/com/scalaSpark/developerCert/Scenario1")

    /*loadRdd.createOrReplaceTempView("task1")
    spark.sql("select order_item_quantity, order_item_subtotal from task1 where order_item_product_price < 400").createOrReplaceTempView("filteredTable")
*/
    spark.stop()
  }

}