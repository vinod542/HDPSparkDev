package com.scalaSpark.developerCert

import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.broadcast.Broadcast

case class strom(Unique_stationID: Long, Latitude: Float, Long: Float, Date: String, Wind_Direction: String, WindSpeed: Integer, Windgusts: Float, Visibility: Float, Temperature: Integer, Dew_Point: Long, AirPressure: Integer)
case class station(Call_number: String, Unique_stationID: Long, Latitude: Float, Long: Float, Elevation: Float, State: String, StationName: String)
object Scenario3 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Scenario 3").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val stromFile = sc.textFile("/Users/vinodkumarvadlamudi/eclipse/Interviews/HDPCSparkDeveloper/src/com/scalaSpark/developerCert/Storms").map { _.split("    ") }
      .map { l => strom(l(0).toLong, l(1).toFloat, l(2).toFloat, l(3).toString(), l(4).toString(), l(5).toInt, l(6).toFloat, l(7).toFloat, l(8).toInt, l(9).toLong, l(10).toInt) }

    val stationFile = sc.textFile("/Users/vinodkumarvadlamudi/eclipse/Interviews/HDPCSparkDeveloper/src/com/scalaSpark/developerCert/station").map { _.split("    ") }
      .map { x => station(x(0), x(1).toLong, x(2).toFloat, x(3).toFloat, x(4).toFloat, x(5), x(6)) }

    val stromRdd = stromFile.map { item => (item.Unique_stationID, item.Latitude, item.Long, item.Date, item.Wind_Direction, item.WindSpeed, item.Windgusts, item.Visibility, item.Temperature, item.Dew_Point, item.AirPressure) }.toDF().as("StromD")
    val stationRdd = stationFile.map { item => (item.Call_number, item.Unique_stationID, item.Latitude, item.Long, item.Elevation, item.State, item.StationName) }.toDF().as("StationD")
    val broadC = sc.broadcast(stationRdd.as("stat"))
    
    sc.stop()

  }

}