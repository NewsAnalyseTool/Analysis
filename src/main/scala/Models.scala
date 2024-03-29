package main

import com.johnsnowlabs.nlp.DocumentAssembler
import com.johnsnowlabs.nlp.annotator.Tokenizer
import com.johnsnowlabs.nlp.annotators.classifier.dl.BertForSequenceClassification
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.johnsnowlabs.nlp.annotator.RoBertaForSequenceClassification
import com.johnsnowlabs.nlp.annotator.Normalizer
import com.johnsnowlabs.nlp.annotator.Lemmatizer
import com.johnsnowlabs.nlp.Finisher
import com.johnsnowlabs.nlp.annotator.StopWordsCleaner
import scala.io.Source

/** An abstraction for the specific models used in the app
  */
trait SentimentModel {

  /** feeds the source dataframe through the model
    *
    * @param df
    *   dataframe containing the text to be analyzed
    * @return
    *   DataFrame with sentiment scores per row
    */
  def transformDataframe(df: DataFrame): DataFrame

  /** extracts only needed columns from the analyzed DataFrame
    *
    * @param df
    *   DataFrame to be filtered
    * @return
    *   DataFame with reduced amount of columns
    */
  def filterColumns(df: DataFrame): DataFrame = {
    import SparkCommons.spark.implicits._

    df.select(
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

/** Build the pipeline for the Tagesschau articles
  *
  * model used:
  * https://sparknlp.org/2021/11/03/bert_sequence_classifier_sentiment_de.html
  */
class TagesschauSentimentModel extends SentimentModel {

  private val documentAssembler = new DocumentAssembler()
    .setInputCol("text")
    .setOutputCol("document")

  private val tokenizer = new Tokenizer()
    .setInputCols("document")
    .setOutputCol("token")

  private val tokenClassifier =
    BertForSequenceClassification
      .load("model/german-news-bert")
      .setInputCols("document", "token")
      .setOutputCol("class")
      .setCaseSensitive(true)
      .setMaxSentenceLength(512)

  private val pipeline = new Pipeline().setStages(
    Array(documentAssembler, tokenizer, tokenClassifier)
  )

  override def transformDataframe(df: DataFrame): DataFrame = {
    filterColumns(
      pipeline
        .fit(df)
        .transform(df)
    )
  }
}

/** Build the Pipeline for the BBC articles
  *
  * model used:
  * https://sparknlp.org/2022/09/19/roberta_classifier_twitter_base_sentiment_latest_en.html
  */
class BbcSentimentModel extends SentimentModel {

  override def transformDataframe(df: DataFrame): DataFrame = {
    filterColumns(
      pipeline
        .fit(df)
        .transform(df)
    )
  }

  private val documentAssembler = new DocumentAssembler()
    .setInputCol("text")
    .setOutputCol("document")

  private val tokenizer = new Tokenizer()
    .setInputCols("document")
    .setOutputCol("token")

  private val seqClassifier = RoBertaForSequenceClassification
    .load("model/reddit-sentiment")
    .setInputCols(Array("document", "token"))
    .setOutputCol("class")

  private val pipeline =
    new Pipeline().setStages(Array(documentAssembler, tokenizer, seqClassifier))

  override def filterColumns(df: DataFrame): DataFrame = {
    import SparkCommons.spark.implicits._

    df.select(
      $"title",
      $"text",
      $"category",
      $"date",
      $"url",
      $"source",
      // class is an array with one entry
      $"class.result" (0).alias("result"),
      // metadata is an array with one entry
      // the single entry stores a map
      $"class.metadata" (0)("Positive").alias("positive"),
      $"class.metadata" (0)("Negative").alias("negative"),
      $"class.metadata" (0)("Neutral").alias("neutral")
    )
  }
}

//
/** Builds the pipeline for the Reddit posts
  *
  * model used:
  * https://sparknlp.org/2022/09/19/roberta_classifier_twitter_base_sentiment_latest_en.html
  */
class RedditSentimentModel extends SentimentModel {

  override def transformDataframe(df: DataFrame): DataFrame = {
    filterColumns(
      pipeline
        .fit(df)
        .transform(df)
    )
  }

  private val documentAssembler = new DocumentAssembler()
    .setInputCol("text")
    .setOutputCol("document")

  private val tokenizer = new Tokenizer()
    .setInputCols("document")
    .setOutputCol("token")

  private val seqClassifier = RoBertaForSequenceClassification
    .load("model/reddit-sentiment")
    .setInputCols(Array("document", "token"))
    .setOutputCol("class")

  private val pipeline =
    new Pipeline().setStages(Array(documentAssembler, tokenizer, seqClassifier))

  override def filterColumns(df: DataFrame): DataFrame = {
    import SparkCommons.spark.implicits._

    df.select(
      $"category",
      $"url",
      $"date",
      $"text",
      $"title",
      $"comments",
      $"source",
      // class is an array with one entry
      $"class.result" (0).alias("result"),
      // metadata is an array with one entry
      // the single entry stores a map
      $"class.metadata" (0)("Positive").alias("positive"),
      $"class.metadata" (0)("Negative").alias("negative"),
      $"class.metadata" (0)("Neutral").alias("neutral")
    )
  }
}

/** Model for getting normalized words intended to use it for most common words
  * analysis
  */
class TextCleanerModel {
  private val documentAssembler = new DocumentAssembler()
    .setInputCol("text")
    .setOutputCol("document")

  private val tokenizer = new Tokenizer()
    .setInputCols("document")
    .setOutputCol("token")

  val exclusionWords = Source.fromFile("./stopwords.txt").getLines().toArray

  private val stopWordsCleaner = new StopWordsCleaner()
    .setStopWords(exclusionWords)
    .setInputCols("token")
    .setOutputCol("cleaned")

  // remove any special characters
  private val normalizer = new Normalizer()
    .setInputCols("cleaned")
    .setOutputCol("normalized")
    .setLowercase(true)
    .setCleanupPatterns(Array("""[^A-Za-z0-9]+"""))

  // make generated words readable
  private val finisher = new Finisher()
    .setInputCols("normalized")
    .setOutputCols("features")

  private val pipeline =
    new Pipeline().setStages(
      Array(
        documentAssembler,
        tokenizer,
        stopWordsCleaner,
        normalizer,
        finisher
      )
    )

  // each row corresponds to one document and the features column
  // contains the words in an array
  def transformData(df: DataFrame): DataFrame = {
    pipeline
      .fit(df)
      .transform(df)
      .select("features")
  }
}
