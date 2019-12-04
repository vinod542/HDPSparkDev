package com.scalaSpark.developer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.compress.GzipCodec

object S1ToS10 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("S1ToS10")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val path = "/Users/vinodkumarvadlamudi/eclipse/Interviews/HDPCSparkDeveloper/src/com/scalaSpark/developer/"
    println("\nScenario1: get action count of S1.txt")
    val s1 = sc.textFile("/Users/vinodkumarvadlamudi/eclipse/Interviews/HDPCSparkDeveloper/src/com/scalaSpark/developer/S1.txt")
    println("Count lines " + s1.count())
    
    println("\nS2: count lines contains HE and doesn't contain HE")
    val filterHE = s1.filter { line => line.toString().contains("HadoopExam") }
    val NoHE = s1.filter { line => !line.toString().contains("HadoopExam") }
    val allHE = s1.filter { line => line.toString().toLowerCase().contains("hadoopexam") }
    println(" HE line count in a given file " + filterHE.count() + " no HE lines " + NoHE.count() + " no case sensitive " + allHE.count())
    
    println("\ns3: count all hadoop keywords from data")
    var data = ("we", "are", "learning", "hadoop", "from", "HadoopExam.", "driving", "we", "are", "learning", "spark", "hadoop", "from", "HadoopExam.", "Hadoop", "hadoop", "hadoopExam")
    val s3 = sc.parallelize(List(data))
    println("count of hadoop words from data: " + s3.filter(word => word.toString().toLowerCase().contains("hadoop")).count())
    
    println("\ns4: concatenate all three files and count lines in all three")
    val f1 = sc.textFile("/Users/vinodkumarvadlamudi/eclipse/Interviews/HDPCSparkDeveloper/src/com/scalaSpark/developer/f1.txt")
    val f2 = sc.textFile("/Users/vinodkumarvadlamudi/eclipse/Interviews/HDPCSparkDeveloper/src/com/scalaSpark/developer/f2.txt")
    val f3 = sc.textFile("/Users/vinodkumarvadlamudi/eclipse/Interviews/HDPCSparkDeveloper/src/com/scalaSpark/developer/f3.txt")
    val f1f2f3 = f1.union(f2).union(f3)
    println("Count of lines in three files: "+f1f2f3.count())
    println("count of all words: "+ f1f2f3.flatMap { word  => word }.count())
    
    println("\ns5: read files under dir to filter hadoop keyword and persist the data")
    val dir = sc.textFile(path+"*.txt")
    val splitting = dir.flatMap { line => line.split(" ") }.filter { word => word.toString().toLowerCase().contains("hadoop") }
    splitting.persist()
    
    println("\ns6: calculate fiinal price using tax")
    val s6 = sc.textFile(path + "/calculate")
    val calculate = s6.map { line => (line,(line.split(",")(1).toDouble + (line.split(",")(1).toDouble / 100 * line.split(",")(2).toDouble))) }
    calculate.saveAsTextFile(path+"/s6Output")
    
    println("\ns7: join three csv files and sorted output based on ID")
    val s7empMgr = sc.textFile(path+"/empMgr")
    val s7empName = sc.textFile(path+"/empName")
    val s7empSal = sc.textFile(path+"/empSal")
    val s7empMgrRdd = s7empMgr.map { line => (line.split(",")(0), line.split(",")(1) )}
    val s7empNameRdd = s7empName.map { line => (line.split(",")(0), line.split(",")(1) )}
    val s7empSalRdd = s7empSal.map { line => (line.split(",")(0), line.split(",")(1) )}
    val joinAll = s7empMgrRdd.join(s7empNameRdd).join(s7empSalRdd)
    val sorted = joinAll.sortByKey()
    val mapping = sorted.map(v => (v._1,v._2._1._1,v._2._1._2,v._2._2))
    mapping.saveAsTextFile(path+"/s7Output")
    
    println("\ns8: load all words from s3  into broadcast var and remove those words from S1.txt file and count the occurance of each word")
    val loadWords = sc.broadcast(s3.collect().toList)
    val loadS1words = s1.flatMap { line => line.split(" ") }
    val removeWords = loadS1words.filter { case (word) => !loadWords.value.contains(word) }
    val occurance = removeWords.map(word => (word,1)).reduceByKey(_+_)
    occurance.collect().foreach(f => println(f))
    
    println("\ns9: load f1,f2,f3.txt files and remove below words from that file. wordcount and save in sorted order using gZIP")
    val giveWords = sc.parallelize(List("a", "the", "an", "as", "with", "this", "these", "is", "are", "in", "for", "to", "and", "the", "of"))
    val loadf1f2f3Rdd = f1f2f3.flatMap { line => line.trim().split(" ") }
    val finalWords = loadf1f2f3Rdd.subtract(giveWords)
    val wordMapping = finalWords.map ( word => (word,1) )
    val wc = wordMapping.reduceByKey(_+_)
    val swapping = wc.map(f => f.swap)
    val sortedResult = swapping.sortByKey(false)
    val finalOutput = sortedResult.map(f => f.swap)
    finalOutput.saveAsTextFile(path+"/s9Output",classOf[GzipCodec])
    
    println("\ns10:load 2files, join and produce (name,sal) ")
    val joiningNSal = s7empNameRdd.join(s7empSalRdd)
    /*scala> joining.collect()
res0: Array[(String, (String, String))] = Array((E09,(Kumar,440000)), (E03,(Amit,15000)), (E07,(Tejas,32000)), (E05,(Dinesh,45000)), (E10,(Venkat,150000)), (E01,(Lokesh,50000)), (E06,(Pavan,42000)), (E02,(Bhupesh,52000)), (E08,(Sheeta,33000)), (E04,(Ratan,53000)))*/
    //save data in multiple file group by salary - means each file has a list of emp names with same sal
    val getValues = joiningNSal.map(line => (line._2._2, line._2._1)) //(or )joiningNSal.values
    // now group by sal and writing into each single RDD
    val salarygby = getValues.groupByKey().collect() //this step is required
    val saving = salarygby.map{case(k,v) => k->sc.makeRDD(v.toSeq)}
    saving.foreach{case(k,rdd) => rdd.saveAsTextFile(path+k)}
    
    sc.stop()
  }
}