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
    pipeline.fit(df).transform(df)
  }
}
