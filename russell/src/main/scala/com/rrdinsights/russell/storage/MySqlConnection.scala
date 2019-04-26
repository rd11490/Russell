package com.rrdinsights.russell.storage

import java.sql.DriverManager
import java.sql.Connection

import com.rrdinsights.russell.utils.Creds._

object MySqlConnection extends AutoCloseable {

  var connection: Connection = null

  def url(database: Database): String =  s"jdbc:mysql://localhost/$database?useSSL=false"

  def getConnection(database: Database): Connection = {
    if (connection == null || connection.isClosed) {
      Class.forName(MySqlConstants.Driver)
      connection = DriverManager.getConnection(url(database), getCreds.MySQL.Username, getCreds.MySQL.Password)
    }
    connection
  }

  override def close(): Unit = {
    if (connection != null && !connection.isClosed) {
      connection.close()
      connection = null
    }
  }
}