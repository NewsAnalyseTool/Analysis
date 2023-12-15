package main

import com.typesafe.config.ConfigFactory

object ConfigLoader {
  val config = ConfigFactory.load()

  val mongoConfig = config.getConfig("mongodb")
  val host = mongoConfig.getString("host")
  val port = mongoConfig.getInt("port")
  val database = mongoConfig.getString("database")
  val username = mongoConfig.getString("username")
  val password = mongoConfig.getString("password")
  val readTagesschau = mongoConfig.getString("readCollection")
  val writeTagesschau = mongoConfig.getString("writeCollection")
  val readReddit = mongoConfig.getString("readReddit")
  val writeReddit = mongoConfig.getString("writeReddit")
}
