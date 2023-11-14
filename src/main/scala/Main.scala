package main

import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

import com.mongodb.spark._

object Main extends App {
  override def main(args: Array[String]): Unit = {
    val appName = "Sentiment Analysis"

    val connectionUri =
      s"mongodb://${ConfigLoader.username}:${ConfigLoader.password}@${ConfigLoader.host}:${ConfigLoader.port}/?authMechanism=SCRAM-SHA-256&authSource=Projektstudium"

    val db = "Projektstudium"

    val collection = "redditTestData"

    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[2]")
      .set("spark.mongodb.read.connection.uri", connectionUri)
      .set("spark.mongodb.read.database", db)
      .set("spark.mongodb.read.collection", collection)

    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()

    val df = spark.read.format("mongodb").load()
    df.show()

    // val pipeline = PipelineModel.load(
    //   "/home/kristiyan/Documents/HTW/5FS/PS/analysis-prototype/spark-nlp/model/"
    // )
    //
    // val result: DataFrame = pipeline.transform(testData)
    // val txt = result.select("class").show(false)
  }
}
