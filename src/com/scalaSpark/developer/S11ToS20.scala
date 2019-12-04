package com.scalaSpark.developer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object S11ToS20 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("S11ToS20")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val path = "/Users/vinodkumarvadlamudi/eclipse/Interviews/HDPCSparkDeveloper/src/com/scalaSpark/developer/"
    println("\ns11: load user.txt file, remove header and create an RDD of values line MAP(id->val,topic->val,hits->val). filter out if id=myself")
    val s11 = sc.textFile(path + "User.txt")
    //write into an RDD
    val s11Rdd = s11.map { line => (line.split(",").map { item => item.trim() }) }
    val takeHeader = s11Rdd.first
    //filter out header to process only data
    val data = s11Rdd.filter(_(0) != takeHeader(0))
    //started mapping -- MAP(id->val,topic->val,hits->val)
    val mapping = data.map { line => takeHeader.zip(line).toMap }
    //Now, filter out "myself"
    val filtering = mapping.filter(line => line("id") != "myself")
    filtering.collect().foreach(f => println(f))

    println("\ns12: load empName, sort by name and save it (id, name) and final result into 1 output file ")
    val empName = sc.textFile(path + "/empName")
    val empNameRdd = empName.map { line => (line.split(",")(1), line.split(",")(0)) }
    val swapped = empNameRdd.sortByKey().map(_.swap)
    //saving result to one file
    swapped.repartition(1).saveAsTextFile(path + "s12Output")

    println("\ns13:load data.txt file, save it(id,(all names of same type)) - in single directory")
    val s13 = sc.textFile(path + "data.txt")
    val s13Rdd = s13.map { line => (line.split(",")(0), line.split(",")(1)) }
    val combinedOutput = s13Rdd.combineByKey(List(_), (x: List[String], y: String) => y :: x, (x: List[String], y: List[String]) => x ::: y)
    combinedOutput.repartition(1).saveAsTextFile(path + "s13Output")

    println("\ns14: put out all valid dates into one file and non-valid into another file from feedback.txt")
    val s14 = sc.textFile(path + "feedback.txt")
    val s14Rdd = s14.map { line => (line.split("|")) }
    //write regular expressions for date to validate
    //11 Jan, 2019
    val reg1 = """(\d+)\s(\w{3})(,)\s(\d{4})""".r
    //Jan 11, 2019
    val reg2 = """(\w{3})\s(\d+)(,)\s(\d{4})""".r
    //11/01/2019
    val reg3 = """(\d+)(\/)(\d+)(\/)(\d{4})""".r
    //15-01-2019 or MM-DD-YYYY
    val reg4 = """(\d{2})(-)(\d{2})(-)(\d{4})""".r

    val validRecord = s14Rdd.filter(date => (
      reg1.pattern.matcher(date(1).trim()).matches() |
      reg2.pattern.matcher(date(1).trim()).matches() |
      reg3.pattern.matcher(date(1).trim()).matches() |
      reg4.pattern.matcher(date(1).trim()).matches()))
    val badRecord = s14Rdd.filter { date =>
      !(
        reg1.pattern.matcher(date(1).trim()).matches() |
        reg2.pattern.matcher(date(1).trim()).matches() |
        reg3.pattern.matcher(date(1).trim()).matches() |
        reg4.pattern.matcher(date(1).trim()).matches())
    }

    //convert array to string
    val valid = validRecord.map { x => (x(0), x(1), x(2)) }
    val bad = badRecord.map { x => (x(0), x(1), x(2)) }
    valid.repartition(1).saveAsTextFile(path + "s14GOutput")
    bad.repartition(1).saveAsTextFile(path + "s14BOutput")

    // println("\ns15: given rdd[Array[Bytes]] - now save this as seqFilen ") .val rdd1 = RDD[Array[Byte]]

    println("\ns16: s16file1 and 2 are given. load and print output lie join oper")
    val s16f1 = sc.textFile(path + "s16file1.txt")
    val s16f2 = sc.textFile(path + "s16file2.txt")
    val s16f1Rdd = s16f1.map(line => line.split(",") match { case Array(a, b, c) => (a, (b, c)) })
    val s16f2Rdd = s16f2.map(line => line.split(",") match { case Array(a, b, c) => (a, (b, c)) })
    s16f1Rdd.join(s16f2Rdd).sortByKey().collect().foreach(f => println(f))

    println("\ns17:given file, load file, should get output like")
    val s17f1 = sc.textFile(path + "s17f1.txt")
    /*//Array[Array[Any]] = 
     * Array(Array(3070811, 1963, 1096, 0, "US", "CA", 0, 1, 0), 
     * Array(3070811, 1963, 1096, 0, "US", "CA", 0, 1, 56), 
     * Array(3070811, 1963, 1096, 0, "US", "CA", 0, 1, 23))
    */
    val splitting = s17f1.map { line => line.split(",") }
    val mapper = splitting.map(line => line.map(x => if (x.isEmpty()) 0 else x))
    mapper.collect().foreach { x => println(x.toList) }

    println("\ns18: union")
    val f18 = sc.parallelize(List(("a", Array(1, 2)), ("b", Array(1, 2))))
    val f182 = sc.parallelize(List(("a", Array(3)), ("b", Array(2))))
    f18.union(f182).collect().foreach(f => println(f))

    println("\nScenario19: load sales.txt,output as CSV with group by dept, desg, state with additional clm with sum(cost 0f company) & total emp cost ")
    val f19 = sc.textFile(path + "sales.txt")
    case class emp(Depart: String, Desig: String, cost: Double, State: String)
    val mapping19 = f19.map(_.split(",")).map(line => emp(line(0), line(1), line(2).toDouble, line(3)))
    //creating a column
    val creation19 = mapping19.map(line => ((line.Depart, line.Desig, line.State), (1, line.cost)))
    val result19 = creation19.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
    val finalMap19 = result19.map(line => (line._1._1, line._1._2, line._1._3, line._2._1, line._2._2))
    finalMap19.repartition(1).saveAsTextFile(path + "output19")

    println("\ns20: given below get Array((1,two,3,4),(1,two,5,6))")
    val f20 = sc.parallelize(Seq(((1, "two"), List((3, 4), (5, 6)))))
    val finalResult = f20.flatMap { case (key, group) => group.map(value => (key._1, key._2, value._1, value._2)) }
    finalResult.collect()

    sc.stop()

  }
}