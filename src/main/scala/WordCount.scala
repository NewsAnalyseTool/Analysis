package main

import com.mongodb.spark.sql._
import org.apache.spark.sql.functions._

// prototype of a word counter in a collection
object WordCount extends App {
  override def main(args: Array[String]): Unit = {

    val sparkCommons = SparkCommons
    val cl = ConfigLoader

    // mongodb connection
    val connectionUri =
      s"mongodb://${cl.username}:${cl.password}@${cl.host}:${cl.port}/?authMechanism=SCRAM-SHA-256&authSource=Projektstudium"

    // local mongodb docker container for testing purposes
    val localReplicaSet =
      "mongodb://mongo1:30001,mongo2:30002,mongo3:30003/?replicaSet=my-replica-set"

    import sparkCommons.spark.implicits._

    val inputDF = sparkCommons.spark.read
      .format("mongodb")
      .option("spark.mongodb.connection.uri", localReplicaSet)
      .option("spark.mongodb.database", "StreamTest")
      .option("spark.mongodb.collection", "tagesschauWordCount")
      .load()

    val model = new TextCleanerModel()

    val wordsDF = model
      .transformData(inputDF)
      .select(explode($"features").as("word"))
      .groupBy("word")
      .agg(count("*").as("count"))
      .sort(desc("count"))
      .limit(20)
  }
}
