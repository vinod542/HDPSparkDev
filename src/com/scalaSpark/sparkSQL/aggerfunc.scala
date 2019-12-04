package com.scalaSpark.sparkSQL

import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import org.apache.spark.SparkContext

object aggerfunc extends UserDefinedAggregateFunction {

  def inputSchema: StructType = StructType(StructField("inputColumn", LongType) :: Nil)

  def bufferSchema: StructType = {
    StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)
  }

  def dataType: DataType = DoubleType

  def deterministic: Boolean = true

  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buffer(0) = buffer.getLong(0) + input.getLong(0)
      buffer(1) = buffer.getLong(1) + 1
    }
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  def evaluate(buffer: Row): Double = buffer.getLong(0).toDouble / buffer.getLong(1)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("SQL aggregator").getOrCreate()

    spark.udf.register("aggerfunc", aggerfunc)
    val ds = spark.read.json("/Users/vinodkumarvadlamudi/Downloads/spark-2.4.3-bin-hadoop2.7/examples/src/main/resources/employees.json")
    ds.createOrReplaceTempView("Employees")
    ds.show()

    val result = spark.sql("SELECT aggerfunc(salary) as average_salary FROM employees")
    result.show()

    spark.stop()
  }

}