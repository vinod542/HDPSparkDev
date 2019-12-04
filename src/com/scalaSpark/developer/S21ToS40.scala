package com.scalaSpark.developer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.immutable.HashSet
import org.apache.commons.lang.mutable.Mutable

case class S27ScoreAvg(Name: String, score: Float)
object S21ToS40 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("S21ToS40").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val path = "/Users/vinodkumarvadlamudi/eclipse/Interviews/HDPCSparkDeveloper/src/com/scalaSpark/developer/"
    println("\nS21: given 3files, highest count in each file with their filenames and highest occuring words")
    val S21f1 = sc.textFile(path + "f1.txt")
    val S21f2 = sc.textFile(path + "f2.txt")
    val S21f3 = sc.textFile(path + "f3.txt")
    val S21f1Rdd = S21f1.flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey(_ + _).map(_.swap).sortByKey(false).map(_.swap)
    val S21f2Rdd = S21f2.flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey(_ + _).map(_.swap).sortByKey(false).map(_.swap)
    val S21f3Rdd = S21f3.flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey(_ + _).map(_.swap).sortByKey(false).map(_.swap)
    val s21Word1 = sc.makeRDD(Array(S21f1.name + "->" + S21f1Rdd.first()._1 + " - " + S21f1Rdd.first()._2))
    val s21Word2 = sc.makeRDD(Array(S21f2.name + "->" + S21f2Rdd.first()._1 + " - " + S21f2Rdd.first()._2))
    val s21Word3 = sc.makeRDD(Array(S21f3.name + "->" + S21f3Rdd.first()._1 + " - " + S21f3Rdd.first()._2))
    val unionAll = s21Word1.union(s21Word2).union(s21Word3)
    unionAll.repartition(1).saveAsTextFile(path + "S21Output")

    println("\nS22: given tech and sal, join both based on first and last name")
    val s22Tech = sc.textFile(path + "tech.txt").map { _.split(",") }.map(line => (line(0), line(1), line(2)))
    val s22Sal = sc.textFile(path + "salary.txt").map { _.split(",") }.map(line => (line(0), line(1), line(2)))
    val s22TechRdd = s22Tech.map { case (a, b, c) => ((a, b), c) }
    val s22SalRdd = s22Sal.map { case (a, b, c) => ((a, b), c) }
    val joiningS22 = s22TechRdd.join(s22SalRdd).map { case ((a, b), (c, d)) => (a, b, c, d) }
    joiningS22.repartition(1).saveAsTextFile(path + "S22Output")

    println("\nS23: below is the given list (name,gender,cost), sum of the cost of name and gender")
    val S23 = List(("Vinod", "Male", 40000), ("Vinod", "Male", 140000), ("Hari", "Female", 4000), ("Hari", "Female", 14000), ("rari", "Female", 4000))
    val S23Rdd = sc.makeRDD(S23)
    val mappingS23 = S23Rdd.map { case (a, b, c) => ((a, b), c) }
    val groupingS23 = mappingS23.groupByKey()
    val finalResultS23 = groupingS23.map { case ((a, b), c) => (a, b, c.sum) }
    finalResultS23.repartition(1).saveAsTextFile(path + "S23Output")

    println("\nS26: given below code to sum of the value by key")
    val S26List = Array("f00=A", "f00=B", "f00=A", "f00=B", "bar=A", "bar=c", "bar=D", "f00=D", "f00=D", "f00=C", "f00=B")
    val S26Rdd = sc.parallelize(S26List)
    val pairingS26 = S26Rdd.map(_.split("=")).map(line => (line(0), line(1)))
    val z = 0
    val aggregateS26 = pairingS26.aggregateByKey(z)((n: Int, v: String) => (n + 1), (p1: Int, p2: Int) => p1 + p2)
    aggregateS26.collect().foreach(f => println(f))

    import scala.collection._
    val initalVal26 = scala.collection.mutable.HashSet.empty[String]
    val initialSet26 = pairingS26.aggregateByKey(initalVal26)((s: mutable.HashSet[String], v: String) => (s += v), (p1: mutable.HashSet[String], p2: mutable.HashSet[String]) => p1 ++= p2)
    initialSet26.collect().foreach(f => println(f))

    val aggreS26 = pairingS26.map { case (a, b) => (a, 1) }
    val otherS26 = aggreS26.reduceByKey(_ + _)
    otherS26.collect().foreach(f => println(f))

    println("\nScenario27: calculate an avg score with  intermediate output")
    val S27 = Array(("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 95.0), ("Wilma", 98.0))
    val s27Rdd = sc.parallelize(S27)
    val mappingVals27 = s27Rdd.mapValues { value => (value, 1) }
    val reducingVals27 = mappingVals27.reduceByKey((l1, l2) => ((l1._1 + l2._1), (l1._2 + l2._2)))
    val avgS27 = reducingVals27.mapValues(value => (value._1 / value._2))
    avgS27.collect().foreach(f => print(f))

    println("\nScenario27: calculate the avg score with combineByKey")
    def createCombiner = ((score: Double) => (1, score))
    type scoreCollector = (Int, Double)
    def mergeValue = (collector: scoreCollector, score: Double) => {
      val (numberScore, totalScore) = collector
      (numberScore + 1, totalScore + score)
    }
    def mergeCombiners = (c1: scoreCollector, c2: scoreCollector) => {
      val (numScore1, totalScore1) = c1
      val (numScore2, totalScore2) = c2
      (numScore1 + numScore2, totalScore1 + totalScore2)
    }
    val combineAvgS27 = s27Rdd.combineByKey(createCombiner, mergeValue, mergeCombiners)
    combineAvgS27.collect().foreach(f => println(f))
    val avgScores27 = combineAvgS27.mapValues(value => (value._2 / value._1))
    avgScores27.collect().foreach(f => println(f))

    println("\nScenario28: cogroup func below")
    val a28 = sc.parallelize(List(1, 2, 1, 3))
    val b28 = a28.map((_, "b"))
    val c28 = a28.map((_, "c"))
    b28.cogroup(c28).foreach(f => println(f))

    println("\nScenario29: countByValue func")
    val b29 = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 2, 4, 6, 1, 2, 1, 1, 1, 1, 1))
    b29.countByValue().foreach(f => print(f._1 + "->" + f._2 + ","))

    println("\nScenario30: write oper1 tto oper2")
    val s30 = sc.parallelize(1 to 10)
    val oper1 = s30.filter(_ % 2 == 0)
    val oper2 = s30.filter(_ < 4)
    print(oper1.collect().toList)
    print(oper2.collect().toList)

    println("\nScenario31: foldByKey example")
    val s31a = sc.parallelize(List("dog", "lion", "cat", "wolf", "panther", "eagle"))
    val s31b = s31a.map { x => (x.length(), x) }
    s31b.foldByKey(" ")(_ + _).collect().foreach(f => println(f))

    println("\nScenario32: full outer join example is below")
    val pair1S32 = sc.parallelize(List(("cat", 2), ("cat", 5), ("book", 4), ("book", 12), ("cat", 1)))
    val pair2S32 = sc.parallelize(List(("mouse", 2), ("cat", 5), ("mouse", 4), ("book", 12), ("mouse", 1)))
    pair1S32.fullOuterJoin(pair2S32).collect()

    println("\nScenario33: glom func usage")
    val S33 = sc.parallelize(1 to 100)
    S33.glom().collect()
    println("\nScenario34: odd and even usage")
    S33.groupBy { x => (if (x % 2 == 0) "even" else "odd") }.collect()
    println("\nScenario35: groupByKey func usage")
    val s35 = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle", "lion", "dog"))
    val addKeyS35 = s35.keyBy(_.length())
    addKeyS35.groupByKey().collect()
    println("\nScenario36: intersection func usage")
    val x36 = sc.parallelize(1 to 25)
    val y36 = sc.parallelize(18 to 30)
    x36.intersection(y36).collect()
    println("\nScenario38: left outer join & right outer func usage")
    pair2S32.leftOuterJoin(pair1S32).collect()
    pair2S32.rightOuterJoin(pair1S32).collect()
    val s39 = s35.map(x => (x.length(), x))
    s39.mapValues { "x" + _ + "x" }.collect()
    s39.reduceByKey(_ + _).collect
    s39.map(_.swap).reduceByKey(_ + _).collect()

    sc.stop()

  }
}