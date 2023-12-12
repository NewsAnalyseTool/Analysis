package main

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.streaming.Trigger

object Main extends App {
  override def main(args: Array[String]): Unit = {

    val appName = "Sentiment Analysis"

    val cl = ConfigLoader

    // mongodb connection
    val connectionUri =
      s"mongodb://${cl.username}:${cl.password}@${cl.host}:${cl.port}/?authMechanism=SCRAM-SHA-256&authSource=Projektstudium"

    // local mongodb docker container for testing purposes
    val localReplicaSet =
      "mongodb://mongo1:30001,mongo2:30002,mongo3:30003/?replicaSet=my-replica-set"

    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[2]") // run locally on 2 cores
      // .set(
      //   "spark.jars",
      //   "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1"
      // )

    val spark = SparkSession
      .builder()
      .appName(appName)
      .config(conf)
      .master("local[*]")
      .getOrCreate()

    // read collection schema is needed for data streaming
    val schema = new StructType()
      .add("source", "string")
      .add("title", "string")
      .add("text", "string")
      .add("category", "string")
      .add("date", "string")
      .add("url", "string")

    val sc = spark.sparkContext

    // pretrained ML model
    val model: SentimentModel = new TagesschauSentimentModel()

    // setup read stream
    val readQuery = spark.readStream
      .format("mongodb")
      .schema(schema)
      .option("spark.mongodb.connection.uri", localReplicaSet)
      .option("spark.mongodb.database", "StreamTest")
      .option("spark.mongodb.collection", "streams")
      .option("spark.mongodb.change.stream.publish.full.document.only", "true")
      .option("checkpointLocation", "../tmp/checkpint/main/read")
      .option("forceDeleteTempCheckpointLocation", "true")
      .load()

    // write stream does not support changing the data
    // thats why data is written in batch mode
    // when a single document comes in it is still being processed
    val writeQuery = readQuery.writeStream
      .foreachBatch((batchDf: DataFrame, batchId: Long) => {
        val analyzedDf: DataFrame = model.transformDataframe(batchDf)
        analyzedDf.write
          .format("mongodb")
          .mode("append")
          .option("spark.mongodb.connection.uri", localReplicaSet)
          .option("spark.mongodb.database", "StreamTest")
          .option("spark.mongodb.collection", "out")
          .save()
      })
      .option("checkpointLocation", "../tmp/checkpoint/main/write")
      .option("forceDeleteTempCheckpointLocation", "true")
      .start()
      .awaitTermination()
    // need a way to close spark context when programm is interrupted instead of quiting without closing
  }
}
