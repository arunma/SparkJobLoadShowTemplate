package edu.nus.bd.ingest

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object DataFrameLoad {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .config(new SparkConf())
      .appName("RDD Load Pipeline")
      .master("local[*]")
      .getOrCreate()

    LogManager.getRootLogger.setLevel(Level.WARN)
    spark.sparkContext.setLogLevel("ERROR")

    val sourceDf: DataFrame = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .option("delimiter", "|")
      .csv("/Users/arunma/IdeaProjects/SparkJobLoadShowTemplate/src/main/resources/NameEmailCountryWithHeaders.csv")


    val totalAge = sourceDf.agg(sum(col("age")))

    totalAge.show()

    Thread.sleep(100000)

  }


}
