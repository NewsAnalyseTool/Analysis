# HTW-STA Analysis

## Description

A module of the HTW Search Trend Analysis Project which takes the data from a
MongoDB and runs different analysis methods on it (e.g Sentiment Analysis, basic
aggregations).

## Getting Started

### Requirements

- Java 11
- sbt 1.9.6

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

- The sentiment analysis model is not included in the repository so it has to be
  downloaded
  [here](https://sparknlp.org/2023/07/28/twitter_xlm_roberta_base_sentiment_en.html).
  Afterwards put it into {project root}/model/ and adjust the path in
  `Main.scala`.
- run the sbt project with `sbt -J-Xmx10G run` to make sure the JVM has enough
  heap space
