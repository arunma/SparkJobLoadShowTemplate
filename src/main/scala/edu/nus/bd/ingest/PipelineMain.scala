package edu.nus.bd.ingest

import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

object PipelineMain {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .config(new SparkConf())
      .appName("Boring Pipeline")
      .master("local[*]")
      .getOrCreate()

    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("email", StringType),
      StructField("age", IntegerType),
      StructField("country", StringType)
    ))

    val sourceRawDf =
      spark
        .read
        .format("csv")
        .option("header", true)
        .option("delimiter", "|")
        .schema(schema)
        .load("/user/arun/source/NameEmailCountry.csv")


    sourceRawDf
      .write
      .format("parquet")
      .option("compression", "snappy")
      .mode(SaveMode.Overwrite)
      .save("/user/arun/demo")


  }

}
