package com.scalaSpark.sparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import com.mysql.jdbc.Driver

case class family(name: String, age: Int)
case class order_items(id: Int, need: Int, code: Int, req: Int, overall: Double, singleP: Double)
case class student(SID: Int, Name: String, Age: Int, Gender: String)
object sqlDF {
  def main(args: Array[String]): Unit = {
    val path = "/Users/vinodkumarvadlamudi/eclipse/Interviews/HDPCSparkDeveloper/src/com/scalaSpark/sparkSQL/"
    val conf = new SparkConf().setAppName("SQL APP").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlc = new SQLContext(sc)
    import sqlc.implicits._
    val hiveC = new HiveContext(sc)

    println("\nCreating a DF or DS from RDD")
    val familyRdd = Seq(family("Jack", 32), family("Jack's Mom", 62), family("Mrs. Jack", 28), family("Jr. Jack", 12))
    val convertingDF = familyRdd.toDF()
    val convertingDS = familyRdd.toDS()
    convertingDF.show()

    println("\nload a CSV file and convert into DF")
    val loadCSV = sc.textFile("/Users/vinodkumarvadlamudi/data-master/retail_db/order_items/part-00001.csv")
    val mappingCSV = loadCSV.map(_.split(",")).map { line => order_items(line(0).toInt, line(1).toInt, line(2).toInt, line(3).toInt, line(4).toDouble, line(5).toDouble) }
    val CSVtoDF = mappingCSV.toDF()
    CSVtoDF.registerTempTable("order_items")
    CSVtoDF.show()

    println("\nloading a json file and convert into DF")
    val loadJson = sqlc.read.json("/Users/vinodkumarvadlamudi/data-master/retail_db_json/order_items/part-00002.json")
    val jsonToDF = loadJson.toDF()
    jsonToDF.show()

    println("\nloading a table from jdbc and convert into DF")
    val jdbcURL = "jdbc:mysql://slave002.hpe.com:3306/hive"
    val tableName = "ROLES"
    val connectionProperties = new java.util.Properties
    connectionProperties.setProperty("driver", "com.mysql.jdbc.Driver")
    connectionProperties.setProperty("user", "hive")
    connectionProperties.setProperty("password", "bigdata")
    val jdbcRdd = sqlc.read.jdbc(jdbcURL, tableName, connectionProperties)
    val jdbcToDF = jdbcRdd.toDF()
    jdbcToDF.show()

    val students = List(student(123, "Kiran", 25, "M"), student(124, "Sun", 20, "M"), student(125, "Kiran", 23, "F"), student(126, "John", 28, "M"))
    val studentDF = students.toDF
    studentDF.dtypes
    studentDF.columns
    studentDF.explain()
    studentDF.explain(true)
    studentDF.printSchema()

    println("\nwrite into the table")
    studentDF.registerTempTable("Stud")
    val studTable = sqlc.sql("SELECT * FROM Stud")
    studTable.toDF().show()

    println("\nwrite into the table from CSV")
    val Order_items = sqlc.sql("SELECT * FROM order_items")
    val orderToDF = Order_items.toDF()
    orderToDF.filter("singleP < 50").show()

    println("\nget only three columns of limit 4")
    val rselect = orderToDF.select("id", "req", "overall")
    rselect.toDF().show()

    sc.stop()
  }
}