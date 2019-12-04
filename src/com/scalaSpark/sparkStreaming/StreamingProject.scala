package com.scalaSpark.sparkStreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.types.StructType

object StreamingProject {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("structured WC").getOrCreate()

    import spark.implicits._

    val socketDF = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
    socketDF.isStreaming
    socketDF.printSchema()

    val userSchema = new StructType().add("name", "string").add("age", "integer")
    val csvDF = spark.readStream.option("sep", ",").schema(userSchema).csv("/Users/vinodkumarvadlamudi/Downloads/spark-2.4.3-bin-hadoop2.7/python/test_support/sql/")
    csvDF.show()

  }

}