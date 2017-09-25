package com.rrdinsights.russell.storage


import java.sql.ResultSet

import com.rrdinsights.russell.storage.tables.MySqlTable
import com.rrdinsights.russell.utils.Control._

import scala.collection.mutable

object MySqlClient {

  /*
    Constants
   */
  private val Drop: String = "DROP"
  private val Create: String = "CREATE"
  private val Table: String = "TABLE"
  private val If: String = "IF"
  private val Not: String = "NOT"
  private val Exists: String = "EXISTS"
  private val Insert: String = "INSERT"
  private val Into: String = "INTO"
  private val Values: String = "VALUES"
  private val Null: String = "NULL"
  private val Select: String = "SELECT"
  private val From: String = "FROM"
  private val And: String = "AND"

  private val DuplicateKey: String = "ON DUPLICATE KEY UPDATE"
  private val IfNotExists: String = s"$If $Not $Exists"

  private val PrimaryKey: String = "primaryKey"

  /*
    Public Methods
   */
  def createTable(table: MySqlTable): Unit =
    using(MySqlConnection.getConnection(Database.nba)) { connection =>
      using(connection.createStatement){ stmt =>
        stmt.execute(createTableStatement(table.name, table.columns))
      }
    }


  def dropTable(table: MySqlTable): Unit =
    using(MySqlConnection.getConnection(Database.nba)) { connection =>
      using(connection.createStatement){ stmt =>
        stmt.execute(dropTableStatement(table.name))
      }
    }

  def insertInto[T <: Product](table: MySqlTable, data: Seq[T]): Unit = {
    data.grouped(10).foreach(insertIntoGrouped(table, _))
  }

  def insertIntoGrouped[T <: Product](table: MySqlTable, data: Seq[T]): Unit = {
    using(MySqlConnection.getConnection(Database.nba)) { connection =>
      using(connection.createStatement){ stmt =>
        stmt.execute(insertTableStatement(table, data))
      }
    }
  }


  def selectFrom[T <: Product](table: MySqlTable, rowMapper: ResultSet => T, whereClauses: String*): Seq[T] = {
    val resultsList = mutable.ListBuffer.empty[T]
    using(MySqlConnection.getConnection(Database.nba)) { connection =>
      using(connection.createStatement) { stmt =>
        val results: ResultSet = stmt.executeQuery(selectTableStatement(table, whereClauses: _*))
        while (results.next()) {
          resultsList += rowMapper(results)
        }
        resultsList
      }
    }
  }
  /*
    Private Methods
   */

  private[storage] def selectTableStatement(table: MySqlTable, whereClauses: String*): String =
    s"$Select ${toColumnNamesForSelect(table)} $From ${table.name}${if (whereClauses.nonEmpty) whereClauses.mkString(" WHERE ", And, "")}"

  private[storage] def createTableStatement(name: String, fields: Seq[SqlTypeHolder]): String =
    s"$Create $Table $IfNotExists $name ${createFieldsStatement(fields)}".trim

  private def primaryKeyStatement(fields: Seq[SqlTypeHolder]): String =
    if (fields.map(_.fieldName).contains(PrimaryKey)) {
      s" PRIMARY KEY ($PrimaryKey)"
    } else {
      ""
    }

  private def dropTableStatement(name: String): String =
    s"$Drop $Table $If $Exists $name"

  private def insertTableStatement[T <: Product](table: MySqlTable, data: Seq[T]): String =
    s"$Insert $Into ${table.name} ${toColumnNamesForInsert(table)} $Values ${toValueRows(data)} $DuplicateKey ${toDuplicateReplaceValues(table.columns)}"

  private def toColumnNames(table: MySqlTable): Seq[String] =
    table.columns.map(_.fieldName)

  private def toColumnNamesForSelect(table: MySqlTable): String =
    toColumnNames(table).mkString(", ")

  private def toColumnNamesForInsert(table: MySqlTable): String =
    toColumnNames(table).mkString("(", ", ", ")")

  private def toValueRows[T <: Product](data: Seq[T]): String =
    data.map(toValueRow(_)).mkString(", ")

  private def toValueRow[T <: Product](data: T): String =
    data.productIterator.map(convertToString).mkString("(", ", ", ")")

  private def toDuplicateReplaceValues(fields: Seq[SqlTypeHolder]): String =
    fields.filterNot(_.fieldName == PrimaryKey).map(toUpdateValue).mkString(", ")

  private def toUpdateValue(field: SqlTypeHolder): String =
    s"${field.fieldName} = $Values(${field.fieldName})"

  private def convertToString(s: Any): String =
    if (s == null) {
      null
    } else {
      s match {
        case str: String =>
          "\'" + cleanString(str) + "\'"
        case _ =>
          s.toString
      }
    }

  private def cleanString(str: String): String = {
    str.replaceAll("'", "")
  }


  private def createFieldsStatement(fields: Seq[SqlTypeHolder]): String =
    if (fields.isEmpty) {
      ""
    } else {
      (fields
        .map(toSqlColumn) :+ primaryKeyStatement(fields))
        .mkString("(", ", ", ")")
    }

  private def toSqlColumn(sqlTypeHolder: SqlTypeHolder): String =
    if (sqlTypeHolder.fieldName == PrimaryKey){
      s"${sqlTypeHolder.sqlColumn} $Not $Null"
    } else {
      sqlTypeHolder.sqlColumn
    }
}