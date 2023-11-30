package main

import com.johnsnowlabs.nlp.annotators.classifier.dl.XlmRoBertaForSequenceClassification
import com.johnsnowlabs.nlp.DocumentAssembler
import com.johnsnowlabs.nlp.annotator.Tokenizer
import com.johnsnowlabs.nlp.annotators.classifier.dl.RoBertaForSequenceClassification
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main extends App {
  override def main(args: Array[String]): Unit = {

    val appName = "Sentiment Analysis"

    val cl = ConfigLoader

    // mongodb connection
    val connectionUri =
      s"mongodb://${cl.username}:${cl.password}@${cl.host}:${cl.port}/?authMechanism=SCRAM-SHA-256&authSource=Projektstudium"

    // spark configuration
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[2]") // run locally on 2 cores
      .set("spark.mongodb.read.connection.uri", connectionUri)
      .set("spark.mongodb.read.database", cl.database)
      .set("spark.mongodb.read.collection", cl.readCollection)
      .set("spark.mongodb.write.connection.uri", connectionUri)
      .set("spark.mongodb.write.database", cl.database)
      .set("spark.mongodb.write.collection", cl.writeCollection)

    val spark = SparkSession
      .builder()
      .appName(appName)
      .config(conf)
      .master("local[*]") // run locally on multiple cores
      .getOrCreate()

    val sc = spark.sparkContext

    // import mongodb collection as df
    val df = spark.read.format("mongodb").load().limit(200)

    val model: SentimentModel = new TagesschauSentimentModel()

    val result = model.transformDataframe(df)
    result.write
      .format("mongodb")
      .mode("append")
      .options(
        Map(
          "uri" -> connectionUri,
          "database" -> cl.database,
          "collection" -> cl.writeCollection
        )
      )
      .save()
  }
}
