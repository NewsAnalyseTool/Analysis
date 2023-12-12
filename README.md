# HTW-STA Analysis

## Description

A module of the HTW Search Trend Analysis Project which takes the data from a
MongoDB and runs different analysis methods on it (e.g Sentiment Analysis, basic
aggregations).

## Getting Started

### Requirements

- JVM 11
- sbt 1.9.6
- Apache Spark 3.2.3 with Hadoop 3.2

### Executing program

- In order to get data from MongoDB you have to configure a connection. This
  should be done in a file called `application.conf` under {project
  root}/src/main/resources/application.conf and should look like this:

```
mongodb {
   host = "HOST-IP"
   port = "PORT"
   database = "DB"
   readCollection = "COLLECTION"
   username = "USERNAME"
   password = "PASSWORD"
   writeCollection = "COLLECTION"
 }
```
- At this point everything happens on a docker container with a 3 node MongoDB for testing purposes
  - if you want to set it up you may have to adjust the `localRepository` String in `Main.scala` because Spark Structured Streaming (the underlying technology of the MongoDB Spark Connector) [works with a DB which has a replica set](https://www.mongodb.com/community/forums/t/mongodb-spark-connector-10-0-2-read-existing-data-as-stream/169149). 
  - in order to set up the docker container have a look at [this](https://github.com/UpSync-Dev/docker-compose-mongo-replica-set)

- For now the App establishes a `readStream` from the MongoDB but processing the data isn't as easy as we thought. In order to write and process the data in near "real time" we have to write in batch mode.
- The streams keep track what data has been processed based on [checkpoints](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#recovering-from-failures-with-checkpointing) which are stored under `../tmp/main/` being outside of the project.
- The sentiment analysis model for reddit data is not included in the repository so it has to be
  downloaded
  [here](https://sparknlp.org/2023/07/28/twitter_xlm_roberta_base_sentiment_en.html).
  Afterwards put it into {project root}/model/ and adjust the path in
  `Main.scala`.
  
- You also have to download the model for the Tagesschau sentiment analyzer which can be found [here](https://sparknlp.org/2021/11/03/bert_sequence_classifier_sentiment_de.html). This should also be moved to the `/model` folder and the path should be adjusted in `Main.scala`.
- run as sbt project
  * with `sbt -J-Xmx10G run` to make sure the JVM has enough heap space
- or alternatively run as spark job
  * you will need to create a fat jar with all the dependencies by running `sbt assembly`
  * this creates the fat jar in the `target/scala-2.12/`
  * then you can run `spark-submit --class main.Main --master local --driver-memory 10G --name SentimentAnalysis --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.1 PATH_TO_FAT_JAR`
  * you can track the progress of the Spark Job with Spark UI on `http://localhost:4040`
