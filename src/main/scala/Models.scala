package main

import com.johnsnowlabs.nlp.DocumentAssembler
import com.johnsnowlabs.nlp.annotator.Tokenizer
import com.johnsnowlabs.nlp.annotators.classifier.dl.{
  BertForSequenceClassification
}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

trait SentimentModel {
  def transformDataframe(df: DataFrame): DataFrame
}

class TagesschauSentimentModel extends SentimentModel {

  val document_assembler = new DocumentAssembler()
    .setInputCol("text")
    .setOutputCol("document")

  val tokenizer = new Tokenizer()
    .setInputCols("document")
    .setOutputCol("token")

  val tokenClassifier =
    BertForSequenceClassification
      .load("model/german-news-bert")
      .setInputCols("document", "token")
      .setOutputCol("class")
      .setCaseSensitive(true)
      .setMaxSentenceLength(512)

  val pipeline = new Pipeline().setStages(
    Array(document_assembler, tokenizer, tokenClassifier)
  )

  override def transformDataframe(df: DataFrame): DataFrame = {
    import SparkCommons.spark.implicits._

    pipeline
      .fit(df)
      .transform(df)
      .select(
        $"source",
        $"title",
        $"text",
        $"category",
        $"date",
        $"url",
        // class is an array with one entry
        $"class.result" (0).alias("result"),
        // metadata is an array with one entry
        // the single entry stores a map
        $"class.metadata" (0)("positive").alias("positive"),
        $"class.metadata" (0)("negative").alias("negative"),
        $"class.metadata" (0)("neutral").alias("neutral")
      )
  }
}
