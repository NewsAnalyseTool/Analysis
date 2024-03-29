package main

import com.typesafe.config.ConfigFactory


/**
  * Container for the environment variables used for the db connection
  */
object ConfigLoader {
  val config = ConfigFactory.load()

  val mongoConfig = config.getConfig("mongodb")
  val host = mongoConfig.getString("host")
  val port = mongoConfig.getInt("port")
  val database = mongoConfig.getString("database")
  val username = mongoConfig.getString("username")
  val password = mongoConfig.getString("password")
  val readTagesschau = mongoConfig.getString("readTagesschau")
  val writeTagesschau = mongoConfig.getString("writeTagesschau")
  val readReddit = mongoConfig.getString("readReddit")
  val writeReddit = mongoConfig.getString("writeReddit")
  val readBbc = mongoConfig.getString("readBbc")
  val writeBbc = mongoConfig.getString("writeBbc")
}
