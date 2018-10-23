package com.rrdinsights.russell.storage.datamodel

import java.io.File
import java.sql.ResultSet
import java.{lang => jl}

trait ResultSetMapper {
  protected def getInt(resultSet: ResultSet, col: Int): jl.Integer = {
    val value = resultSet.getInt(col + 1)
    if (resultSet.wasNull) {
      null
    } else {
      jl.Integer.valueOf(value)
    }
  }

  protected def getLong(resultSet: ResultSet, col: Int): jl.Long = {
    val value = resultSet.getLong(col + 1)
    if (resultSet.wasNull) {
      null
    } else {
      jl.Long.valueOf(value)
    }
  }

  protected def getString(resultSet: ResultSet, col: Int): String =
    resultSet.getString(col + 1)

  protected def getBoolean(resultSet: ResultSet, col: Int): jl.Boolean = {
    val value = resultSet.getBoolean(col + 1)
    if (resultSet.wasNull()) {
      null
    } else {
      jl.Boolean.valueOf(value)
    }
  }

  protected def getDouble(resultSet: ResultSet, col: Int): jl.Double = {
    val value = resultSet.getDouble(col + 1)
    if (resultSet.wasNull()) {
      null
    } else {
      jl.Double.valueOf(value)
    }
  }
}