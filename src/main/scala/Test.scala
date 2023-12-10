package main

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

// used to try things out
object Test extends App {
  override def main(args: Array[String]): Unit = {

    val appName = "Sentiment Analysis"

    val cl = ConfigLoader

    // mongodb connection
    val connectionUri =
      s"mongodb://${cl.username}:${cl.password}@${cl.host}:${cl.port}/?authMechanism=SCRAM-SHA-256&authSource=Projektstudium"

    val localReplicaSet =
      "mongodb://mongo1:30001,mongo2:30002,mongo3:30003/?replicaSet=my-replica-set"

    // spark configuration
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[2]") // run locally on 2 cores

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

    val query = spark.readStream
      .format("mongodb")
      .schema(schema)
      .option("spark.mongodb.connection.uri", localReplicaSet)
      .option("spark.mongodb.database", "StreamTest")
      .option("spark.mongodb.collection", "streams")
      .option("spark.mongodb.change.stream.publish.full.document.only", "true")
      .option("forceDeleteTempCheckpointLocation", "true")
      .load()

    val query2 = query.writeStream
      .format("mongodb")
      .queryName("ToMDB")
      .option("checkpointLocation", "/tmp/")
      .option("forceDeleteTempCheckpointLocation", "true")
      .option("spark.mongodb.connection.uri", localReplicaSet)
      .option("spark.mongodb.database", "StreamTest")
      .option("spark.mongodb.collection", "out")
      .outputMode("append")
      .start()
      .awaitTermination()
  }
}
