package com.scalaSpark.developerCert

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._

object Scenario4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("scenario4").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val ownSchema = new StructType().add(name = "Day", StringType).add(name = "Euro", FloatType).add(name = "Yen", FloatType).add(name = "Dollar", FloatType)
    import spark.implicits._
    val loadCSV = spark.read.format("csv").schema(ownSchema).load("/Users/vinodkumarvadlamudi/eclipse/Interviews/HDPCSparkDeveloper/src/com/scalaSpark/developerCert/Exchange.csv").toDF()
    val renaming = loadCSV.select(col("Day"), to_date(col("Day"), "yyyy/MM/dd").as("Date"), col("Euro").alias("euro"), col("Dollar").alias("dollar"))
    val Rnaming = renaming.select(year($"Date").alias("Date"), $"euro", $"dollar")
    Rnaming.select(col("Date"), col("euro"), col("dollar")).createOrReplaceTempView("TabAvg")
    
    spark.sql("select avg(euro), avg(dollar) from TabAvg where Date = 2017").repartition(1).write.option("sep", "\t").option("header", true).csv("/Users/vinodkumarvadlamudi/eclipse/Interviews/HDPCSparkDeveloper/src/com/scalaSpark/developerCert/OutputScenario4")

    spark.sparkContext.stop()
  }
}