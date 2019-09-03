package edu.nus.bd.ingest

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RDDLoad {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .config(new SparkConf())
      .appName("RDD Load Pipeline")
      .master("local[*]")
      .getOrCreate()

    LogManager.getRootLogger.setLevel(Level.WARN)
    spark.sparkContext.setLogLevel("ERROR")

    val lines: RDD[String] = spark.sparkContext.textFile("/Users/arunma/IdeaProjects/SparkJobLoadShowTemplate/src/main/resources/NameEmailCountry.csv", 3)

    val agesAsStrings = lines.map(line => line.split("\\|")(2))

    val agesAsNumbers = agesAsStrings.map(ageStr => ageStr.toInt)

    val totalAge = agesAsNumbers.reduce((one, two) => one + two)

    print (s"Total Age : $totalAge")

    Thread.sleep(100000)

  }


}
