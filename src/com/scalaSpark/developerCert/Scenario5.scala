package com.scalaSpark.developerCert

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Scenario5 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Scenario5").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val prepareSchema = new StructType().add(name = "Station ID", IntegerType).add(name = "Lat", DoubleType).add(name = "Long", DoubleType).add(name = "Date", DateType)
      .add(name = "Wind Direction", StringType).add(name = "Wind Speed", IntegerType).add(name = "wind gusts", IntegerType).add(name = "Visibility", StringType)
      .add(name = "Temp", IntegerType).add(name = "DewPoint", LongType).add(name = "Air Pressure", LongType)
    val streamingData = spark.readStream.schema(prepareSchema).csv("/Users/vinodkumarvadlamudi/Documents/")
    val aggregateData = streamingData.groupBy("Station ID", "Temp")
    val aggregateData1 = aggregateData.agg(avg("Temp"))
    val query = aggregateData1.writeStream.format("console").outputMode("complete").start()
    query.awaitTermination()

  }

}