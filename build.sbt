scalaVersion := "2.12.18"

lazy val root = project
  .in(file("."))
  .settings(
    name := "hello-scala",
    version := "0.1.0-SNAPSHOT",
    libraryDependencies ++= Seq(
      // config for db
      "com.typesafe" % "config" % "1.4.3",
      "org.mongodb.spark" % "mongo-spark-connector_2.12" % "10.2.1",
      "org.apache.spark" %% "spark-core" % "3.2.3",
      "org.apache.spark" %% "spark-mllib" % "3.2.3",
      "com.johnsnowlabs.nlp" %% "spark-nlp" % "4.3.0"
    )
  )

// include the 'provided' Spark dependency on the classpath for  sbt run
// Compile / run := Defaults
//   .runTask(
//     Compile / fullClasspath,
//     Compile / run / mainClass,
//     Compile / run / runner
//   )
//   .evaluated
