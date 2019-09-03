package edu.nus.bd.ingest

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object DemoDataframe extends App {

  val spark = SparkSession
    .builder()
    .config(new SparkConf())
    .appName("Demo")
    .master("local[*]")
    .getOrCreate()

  LogManager.getRootLogger.setLevel(Level.WARN)
  spark.sparkContext.setLogLevel("ERROR")

  val demographicsJsonDf = spark
    .read
    .option("multiline", true)
    .json("/Users/arunma/IdeaProjects/SparkJobLoadShowTemplate/src/main/resources/demographics.json")

  //Show sample data
  demographicsJsonDf.show(false)

  //select
  demographicsJsonDf.select("accountID", "creditLimit", "education")

  //Select Expr
  demographicsJsonDf.selectExpr("accountID", "creditLimit", "creditLimit * 100")

  //Filter
  demographicsJsonDf.filter(col("creditLimit")>0).show(false)

  //Order by
  demographicsJsonDf.orderBy(col("creditLimit")).show(false)
  demographicsJsonDf.orderBy(col("creditLimit").desc).show(false)

  //Limit
  demographicsJsonDf.orderBy(col("creditLimit").desc).limit(10)


  //With column
  demographicsJsonDf.withColumn("creditLimitTimes10", col("creditLimit") * 10).show(false)
  demographicsJsonDf.withColumn("today", current_date()).show(false)
  demographicsJsonDf.withColumn("dummy_placeholder", lit("Arun")).show(false)

  //With column renamed
  demographicsJsonDf.withColumnRenamed("creditLimit", "creditScore").show(false)

  //Describe
  demographicsJsonDf.describe("creditLimit", "accountID")

  //Standard deviation
  demographicsJsonDf.groupBy("accountID").agg(mean("creditLimit")).show(false)









  //Print the schema
  demographicsJsonDf.printSchema()

  //Read from CSV
  val transactionsCsvDf = spark
    .read
    .format("csv")
    .option("header", true)
    .option("delimiter", ",")
    .option("inferSchema", true)
    .load("/Users/arunma/IdeaProjects/SparkJobLoadShowTemplate/src/main/resources/transactions-2019_04_01.csv")

  transactionsCsvDf.printSchema()

  //Join demographics and transactions - inner, left, outer  - https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-joins.html
  val joined = demographicsJsonDf.join(transactionsCsvDf, Seq("accountID"))


  joined.groupBy("accountID").sum("transactionAmountUSD")
    .show(false)

  joined.groupBy("accountID").agg(sum("transactionAmountUSD"))
    .show(false)

  //Create queryable view
  joined.createOrReplaceTempView("joined")
  val accountTransactions = spark.sql("select accountID, sum(transactionAmountUSD) total from joined group by accountID order by total desc ")

  //Filter for a specific account
  val account90 = joined.filter(col("accountID") === "A1899946977632390")


  joined.filter(col("accountID") === "A1899946977632390").groupBy("accountID").sum("transactionAmountUSD") //2.0999999999999996


  //Collect list
  account90.groupBy("accountID").agg(collect_list("transactionAmountUSD")).show(false)


  //Read from CSV
  val transactionsCsvDfRaw = spark
    .read
    .format("csv")
    .option("header", true)
    .option("delimiter", ",")
    .load("/Users/arunma/IdeaProjects/SparkJobLoadShowTemplate/src/main/resources/transactions-2019_04_01.csv")



  import org.apache.spark.sql.types._
  val transactionSchema =
    StructType(Seq(
      StructField("transactionID", StringType),
      StructField("accountID", StringType),
      StructField("transactionAmountUSD", DoubleType),
      StructField("transactionCurrencyCode", StringType),
      StructField("transactionDate", StringType),
      StructField("transactionScenario", StringType),
      StructField("transactionType", StringType),
      StructField("cardType", StringType),
      StructField("paymentBillingState", StringType),
      StructField("paymentBillingCountryCode", StringType),
      StructField("merchantIndustry", StringType)
    ))

  val transactionsCsvDfWithSchema = spark
    .read
    .format("csv")
    .option("header", true)
    .option("delimiter", ",")
    .schema(transactionSchema)
    .load("/Users/arunma/IdeaProjects/SparkJobLoadShowTemplate/src/main/resources/transactions-2019_04_01.csv")




}
