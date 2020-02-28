package edu.nus.bd.ingest

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object DataFrameLoad {
  def main(args: Array[String]): Unit = {

    //val filePath="/Users/arunma/IdeaProjects/SparkJobLoadShowTemplate/src/main/resources/NameEmailCountryWithHeaders.csv"
    val filePath = args(0)

    val spark = SparkSession
      .builder()
      .config(new SparkConf())
      .appName("DataFrame Load Pipeline")
      //.master("local[*]")
      .getOrCreate()

    LogManager.getRootLogger.setLevel(Level.WARN)
    spark.sparkContext.setLogLevel("ERROR")

    val sourceDf: DataFrame = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .option("delimiter", ",")
      .format("csv")
      .load(filePath)


    //val totalAge = sourceDf.agg(sum(col("age")))
    sourceDf.show()
    Thread.sleep(100000)
    spark.stop()
  }


}
