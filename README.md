# HTW-STA Analysis

## Description

A module of the HTW Search Trend Analysis Project which takes the data from a MongoDB and runs different analysis methods on it (e.g Sentiment Analysis, basic aggregations).

## Getting Started

### Requirements

* Java 11
* sbt 1.9.6


### Executing program

* The sentiment analysis model is not included in the repository so it has to be downloaded [here](https://sparknlp.org/2022/09/19/roberta_classifier_twitter_base_sentiment_latest_en.html). Afterwards put it into {project root}/model/ and adjust the path in ``` Main.scala ```.
* run the sbt project with ```sbt -J-Xmx10G run ``` to make sure the JVM has enough heap space

