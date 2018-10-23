package com.rrdinsights.russell.utils

import java.io.InputStream

import org.json4s.DefaultFormats
import org.json4s._
import org.json4s.jackson.JsonMethods._


object Creds {
  implicit val defaultFormats: DefaultFormats.type = DefaultFormats
  private val fileLocation: String = "MySqlCred.json"
  private lazy val creds: Creds =
    parse(scala.io.Source.fromInputStream(getStream()).getLines.mkString).extract[Creds]

  private def getStream( /*IO*/ ): InputStream = {
    val out = getClass.getClassLoader.getResourceAsStream(fileLocation)
    println(out)
    out
  }

  def getCreds: Creds = creds
}

final case class Creds(MySQL: MySQLCred, ShotSite: ShotSiteCred)

final case class MySQLCred(Username: String, Password: String)
final case class ShotSiteCred(Auth: String, Href: String)