package main

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object SparkCommons {
  val appName: String = "Sentiment Analysis"
  val conf = new SparkConf()
    .setAppName(appName)
    .setMaster("local[*]")

  val spark = SparkSession
    .builder()
    .appName(appName)
    .config(conf)
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  val schema = new StructType()
    .add("source", "string")
    .add("title", "string")
    .add("text", "string")
    .add("category", "string")
    .add("date", "string")
    .add("url", "string")
}
