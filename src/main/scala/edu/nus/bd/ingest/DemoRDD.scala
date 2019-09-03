package edu.nus.bd.ingest

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object DemoRDD {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .config(new SparkConf())
      .appName("Demo")
      .master("local[*]")
      .getOrCreate()

    LogManager.getRootLogger.setLevel(Level.WARN)
    spark.sparkContext.setLogLevel("ERROR")

    val lines = spark.sparkContext.textFile("/Users/arunma/IdeaProjects/SparkJobLoadShowTemplate/src/main/resources/NameEmailCountry.csv", 3)

    val agesAsStrings = lines.map(line => line.split(",")(2))

    val agesAsNumbers = agesAsStrings.map(ageStr => ageStr.toInt)

    val totalAge = agesAsNumbers.reduce((one, two) => one + two)

    print (s"Total Age : $totalAge")

    Thread.sleep(100000)


    lines.count()
    lines.take(2)
    lines.collect()

    //lines.collect() Show type


    //Creating a PairRDD
    val agesToNames = lines.map(line => (line.split(",")(2), line.split(",")(0)))

    agesToNames.reduceByKey((v1, v2) => v1 + "|" + v2)

    //Finding number of people in the same age
    val total = agesToNames.mapValues (x => 1).reduceByKey((v1, v2) => v1 + v2)
    total.collect()


    //Take ordered
    implicit object AgeOrdering extends Ordering[(String,Int)]{
      override def compare(x: (String, Int), y: (String, Int)): Int = y._2 compare x._2
    }

    total.takeOrdered(5)


    print (totalAge)


    val text = spark.sparkContext.textFile("/Users/arunma/IdeaProjects/SparkJobLoadShowTemplate/src/main/resources/randomText.txt")

    text.take(2)


    val arrayOfStrings = text.map(line => line.split((" ")))

    val flattenedStrings = text.flatMap(line => line.split((" ")))

    val lowerCasedStrings = flattenedStrings.map(eachString => eachString.toLowerCase)

    val pairStrings = lowerCasedStrings.map(each => (each, 1))

    val totalStrings = pairStrings.reduceByKey((v1, v2) => v1 + v2)


    implicit object StringIntTupleOrdering extends Ordering[(String,Int)]{
      override def compare(x: (String, Int), y: (String, Int)): Int = y._2 compare x._2
    }

    val stringIntTupleOrdering = StringIntTupleOrdering
    totalStrings.takeOrdered(10)(stringIntTupleOrdering)



    /*

    scala> totalStrings.toDebugString
res23: String =
(2) ShuffledRDD[35] at reduceByKey at <console>:30 []
 +-(2) MapPartitionsRDD[34] at map at <console>:30 []
    |  MapPartitionsRDD[33] at map at <console>:30 []
    |  MapPartitionsRDD[32] at flatMap at <console>:30 []
    |  /Users/arunma/IdeaProjects/SparkJobLoadShowTemplate/src/main/resources/randomText.txt MapPartitionsRDD[14] at textFile at <console>:29 []
    |  /Users/arunma/IdeaProjects/SparkJobLoadShowTemplate/src/main/resources/randomText.txt HadoopRDD[13] at textFile at <console>:29 []


     */

  }


}
