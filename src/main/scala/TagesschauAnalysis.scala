package main

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset

/** Spark Job implementation of the Tagesschau sentiment analysis
  */
object TagesschauSparkJob extends App {
  override def main(args: Array[String]): Unit = {

    val sparkCommons = SparkCommons
    val cl = ConfigLoader

    // mongodb connection
    val connectionUri =
      s"mongodb://${cl.username}:${cl.password}@${cl.host}:${cl.port}/?authMechanism=SCRAM-SHA-256&authSource=admin"

    // pretrained ML model
    val model: SentimentModel = new TagesschauSentimentModel()

    // setup read stream
    val readQuery = sparkCommons.spark.readStream
      .format("mongodb")
      .schema(sparkCommons.schema)
      .option("spark.mongodb.connection.uri", connectionUri)
      .option("spark.mongodb.database", "Tagesschau")
      .option("spark.mongodb.collection", cl.readTagesschau)
      .option("spark.mongodb.change.stream.publish.full.document.only", "true")
      .option("checkpointLocation", "../tmp/checkpint/tagesschau/read")
      .option("forceDeleteTempCheckpointLocation", "true")
      .load()

    // write stream does not support changing the data
    // thats why data is written in batch mode
    // when a single document comes in it is still being processed
    val writeQuery = readQuery.writeStream
      .foreachBatch((batchDf: DataFrame, batchId: Long) => {
        val analyzedDf = model
          .transformDataframe(batchDf)

        analyzedDf.write
          .format("mongodb")
          .mode("append")
          .option("spark.mongodb.connection.uri", connectionUri)
          .option("spark.mongodb.database", "Tagesschau")
          .option("spark.mongodb.collection", cl.writeTagesschau)
          .save()
      })
      .option("checkpointLocation", "../tmp/checkpoint/tagesschau/write")
      .option("forceDeleteTempCheckpointLocation", "true")
      .start()
      .awaitTermination()
  }
}
