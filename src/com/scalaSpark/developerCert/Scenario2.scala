package com.scalaSpark.developerCert

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

case class csvReader(Date: String, RG01: Double, RG02: Double, RG03: Double, RG04: Double, RG05: Double, RG07: Double, RG08: Double, RG09: Double, RG10_30: Double, RG11: Double, RG12: Double, RG14: Double, RG15: Double, RG16: Double, RG17: Double, RG18: Double, RG20_25: Double)

object Scenario2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("scenario2").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val loadRdd = sc.textFile("/Users/vinodkumarvadlamudi/eclipse/Interviews/HDPCSparkDeveloper/src/com/scalaSpark/developerCert/Scenario2.csv")
    val header = loadRdd.first()
    val data = loadRdd.filter(_.split(",")(0) != header.split(",")(0))
    val csvRdd = data.map(_.split(",")).map(line => csvReader(line(0).toString, line(1).toDouble, line(2).toDouble, line(3).toDouble, line(4).toDouble, line(5).toDouble, line(6).toDouble, line(7).toDouble, line(8).toDouble, line(9).toDouble, line(10).toDouble, line(11).toDouble, line(12).toDouble, line(13).toDouble, line(14).toDouble, line(15).toDouble, line(16).toDouble, line(17).toDouble))
    val csvRddDF = csvRdd.toDF()
    //csvRddDF.show()

    //other way 
    val spark = SparkSession.builder().getOrCreate()
    val csvDF = spark.read.format("csv").option("header", "true").option("interSchema", "true").load("/Users/vinodkumarvadlamudi/eclipse/Interviews/HDPCSparkDeveloper/src/com/scalaSpark/developerCert/Scenario2.csv").toDF()
    //filter out row where "RG10_30" <= 2.50
    val step2 = csvDF.where($"RG10_30" <= 2.50 || $"year(from_unixtime(unix_timestamp(Date, 'MM/dd/yyyy')))" != 2005)
    //step3: sort by Double value in RG09 in asc and RG20_25 in asc
    val step3 = step2.sort($"RG09", $"RG20_25")
    //print only Date, RG03, RG09, RG10_30, RG20_25
    step3.select("Date", "RG03", "RG09", "RG10_30", "RG20_25").repartition(1).write.option("header", "true").csv("/Users/vinodkumarvadlamudi/eclipse/Interviews/HDPCSparkDeveloper/src/com/scalaSpark/developerCert/Output2")
    //csvDF.filter($"RG10_30" <= 2.50).createOrReplaceTempView("filtered")
    //sqlContext.sql("select * from filtered").show()
    //sqlContext.sql("select * from filtered where year(from_unixtime(unix_timestamp(Date, 'MM/dd/yyyy'))) != 2005").show()

    sc.stop()
  }
}