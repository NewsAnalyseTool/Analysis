package main

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

    // mongodb connection
    val connectionUri =
      s"mongodb://${ConfigLoader.username}:${ConfigLoader.password}@${ConfigLoader.host}:${ConfigLoader.port}/?authMechanism=SCRAM-SHA-256&authSource=Projektstudium"

    // spark configuration
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[2]")
      .set("spark.mongodb.read.connection.uri", connectionUri)
      .set("spark.mongodb.read.database", ConfigLoader.database)
      .set("spark.mongodb.read.collection", ConfigLoader.readCollection)
      .set("spark.mongodb.write.connection.uri", connectionUri)
      .set("spark.mongodb.write.database", ConfigLoader.database)
      .set("spark.mongodb.write.collection", ConfigLoader.writeCollection)

    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()

    // import mongodb collection as df
    val df = spark.read.format("mongodb").load().limit(20)

    val aggregate: DataFrame = groupByColumn(df, "Kategorie")
    aggregate.write
      .format("mongodb")
      .mode("append")
      .options(
        Map(
          "uri" -> connectionUri,
          "database" -> ConfigLoader.database,
          "collection" -> "redditTestAnalyse"
        )
      )
      .save()
  }

  // minimal goal for this sprint -> proof of concept for reading and writing in db
  def groupByColumn(df: DataFrame, column: String): DataFrame = {
    df.groupBy(column).agg(count(column).as("count"))
  }

  // separated the ML stuff for later use
  def launchModel(df: DataFrame): Unit = {
    // model configuration
    val documentAssembler =
      new DocumentAssembler()
        .setInputCol("Titel")
        .setOutputCol("document")

    val tokenizer = new Tokenizer()
      .setInputCols("document")
      .setOutputCol("token")

    // load classifier
    val seq_classifier = RoBertaForSequenceClassification
      .load(
        "/home/kristiyan/Documents/HTW/5FS/PS/analysis-prototype/spark-nlp/model/roberta-class-twitter-base/"
      )
      .setInputCols(Array("document", "token"))
      .setOutputCol("class")

    // assemble the pipeline
    val pipeline = new Pipeline().setStages(
      Array(documentAssembler, tokenizer, seq_classifier)
    )

    // resulting df
    val result: DataFrame = pipeline.fit(df).transform(df)
    result.select("Titel", "class").show(truncate = false)
  }
}
