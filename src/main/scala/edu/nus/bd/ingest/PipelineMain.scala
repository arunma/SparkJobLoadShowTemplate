package edu.nus.bd.ingest

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types._

object PipelineMain {
  def main(args: Array[String]): Unit = {

    val readFilePath = args(0)
    val outputFilePath = args(1)

    val sparkConf = buildSparkConf()

    implicit val spark = SparkSession
      .builder()
      .config(sparkConf)
      .appName("Boring Pipeline")
      .master("local[*]")
      .getOrCreate()
    LogManager.getRootLogger.setLevel(Level.WARN)
    spark.sparkContext.setLogLevel("ERROR")

    runPipeline(readFilePath, outputFilePath)

  }

  private def runPipeline(readFilePath: String, outputFilePath: String)(implicit spark: SparkSession) = {
    val inputDf = readFile(readFilePath)
    showData(inputDf)
  }

  private def readFile(filePath: String)(implicit spark: SparkSession) = {

    val schema = StructType(Seq(
      StructField("url", StringType),
      StructField("urlid", IntegerType),
      StructField("alchemy_category", StringType),
      StructField("is_news", StringType),
      StructField("lengthyLinkDomain", IntegerType),
      StructField("news_front_page", StringType),
      StructField("non_markup_alphanum_characters", LongType),
      StructField("numberOfLinks", IntegerType),
      StructField("numwords_in_url", IntegerType),
      StructField("spelling_errors_ratio", DoubleType)
    ))

    val sourceRawDf =
      spark
        .read
        .format("csv")
        .option("header", true)
        .option("delimiter", "\t")
        .schema(schema)
        .load(filePath)

    sourceRawDf
  }


  def showData(inputDf: DataFrame)(implicit spark: SparkSession): Unit = {
    inputDf.printSchema()
    inputDf.show(false)
  }

  def buildSparkConf(): SparkConf = new SparkConf()
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    .set("spark.sql.streaming.schemaInference", "true")
    .set("spark.sql.autoBroadcastJoinThreshold", "-1")
    .set("spark.scheduler.mode", "FAIR")
    .set("spark.sql.warehouse.dir", "/tmp/awaywarehose")
    .set("spark.kryoserializer.buffer.max", "1g")
}
