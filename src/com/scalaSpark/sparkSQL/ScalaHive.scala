package com.scalaSpark.sparkSQL

import java.io.File
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode

case class Record(key: Int, value: String)
object ScalaHive {
  def main(args: Array[String]): Unit = {

    val warehouseLocation = new File("warehouse").getAbsolutePath
    val spark = SparkSession.builder().appName("Hive Practice").master("local").config("spark.sql.dir.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    sql("create table if not exists src (key INT, value STRING) using hive")
    sql("load data local inpath '/Users/vinodkumarvadlamudi/eclipse/Interviews/WhizlabsT/spark-warehouse/src/kv1.txt' into table src")
    //sql("select * from src").show()
    //sql("select count(*) from src").show()
    val sqlDF = sql("select Key, value from src where key < 10 order by key")
    val stringDS = sqlDF.map { case Row(key: Int, value: String) => s"key: $key, value: $value" }
    //stringDS.show()
    val recordDF = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
    recordDF.createOrReplaceTempView("records")
    sql("select * from records join src on records.key = src.key").show()
    sql("create table hive_record_parquet(key int, value string) stored as parquet")
    sql("create table hive_record_ORC(key int, value string) stored as orc")
    val df = spark.table("src")
    df.write.mode(SaveMode.Overwrite).saveAsTable("hive_records")
    spark.stop()

  }
}