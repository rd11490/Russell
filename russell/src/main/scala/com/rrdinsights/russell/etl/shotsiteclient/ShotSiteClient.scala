package com.rrdinsights.russell.etl.shotsiteclient

import com.rrdinsights.russell.etl.application.{PlayerInfo, TeamInfo}
import com.rrdinsights.russell.storage.datamodel.{
  RealAdjustedFourFactorsForSite,
  ShotWithPlayers
}
import com.rrdinsights.russell.utils.Control._
import com.rrdinsights.russell.utils.Creds
import com.rrdinsights.scalabrine.parameters.{Parameter, ParameterValue}
import org.apache.http.client.config.{CookieSpecs, RequestConfig}
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.http.message.BasicHeader

import scala.collection.JavaConverters._

object ShotSiteClient {

  private def buildClient: CloseableHttpClient = {
    val httpParams = RequestConfig
      .custom()
      .setCookieSpec(CookieSpecs.STANDARD)
      .setConnectionRequestTimeout(20000)
      .setSocketTimeout(20000)
      .setConnectTimeout(20000)
      .build()

    HttpClientBuilder
      .create()
      .setDefaultHeaders(Headers.headers)
      .setDefaultRequestConfig(httpParams)
      .build()
  }

  def postFourFactors(
      fourFactors: Seq[RealAdjustedFourFactorsForSite]): Unit = {
    using(buildClient) { client =>
      try {
        println("Posting Shots")
        val post =
          new HttpPost(s"${Creds.getCreds.ShotSite.Href}/addfourfactors")
        val entity =
          new StringEntity(RealAdjustedFourFactorsForSite.toJson(fourFactors))
        post.setEntity(entity)
        val resp = client.execute(post)
        println(resp)
        resp.close()
      } catch {
        case e: Throwable =>
          println(e)
          throw e
      }
    }
  }

  def postFourFactors3Yr(
                       fourFactors: Seq[RealAdjustedFourFactorsForSite]): Unit = {
    using(buildClient) { client =>
      try {
        println("Posting Shots")
        val post =
          new HttpPost(s"${Creds.getCreds.ShotSite.Href}/addfourfactors3")
        val entity =
          new StringEntity(RealAdjustedFourFactorsForSite.toJson(fourFactors))
        post.setEntity(entity)
        val resp = client.execute(post)
        println(resp)
        resp.close()
      } catch {
        case e: Throwable =>
          println(e)
          throw e
      }
    }
  }

  def postFourFactors5Yr(
                          fourFactors: Seq[RealAdjustedFourFactorsForSite]): Unit = {
    using(buildClient) { client =>
      try {
        println("Posting Shots")
        val post =
          new HttpPost(s"${Creds.getCreds.ShotSite.Href}/addfourfactors5")
        val entity =
          new StringEntity(RealAdjustedFourFactorsForSite.toJson(fourFactors))
        post.setEntity(entity)
        val resp = client.execute(post)
        println(resp)
        resp.close()
      } catch {
        case e: Throwable =>
          println(e)
          throw e
      }
    }
  }

  def postShots(shot: Seq[ShotWithPlayers]): Unit = {

    using(buildClient) { client =>
      try {
        println("Posting Shots")
        val post = new HttpPost(s"${Creds.getCreds.ShotSite.Href}/addraws")
        val entity = new StringEntity(ShotWithPlayers.shotsToJson(shot))
        post.setEntity(entity)
        val resp = client.execute(post)
        println(resp)
        resp.close()
      } catch {
        case e: Throwable =>
          println(e)
          throw e
      }
    }
  }

  def postPlayers(shot: Seq[PlayerInfo]): Unit = {
    using(buildClient) { client =>
      try {
        println("Posting Shots")
        val post = new HttpPost(s"${Creds.getCreds.ShotSite.Href}/addplayers")
        val entity = new StringEntity(PlayerInfo.toJson(shot))
        post.setEntity(entity)
        val resp = client.execute(post)
        println(resp)
        resp.close()
      } catch {
        case e: Throwable =>
          println(e)
          throw e
      }
    }
  }

  def postTeams(teams: Seq[TeamInfo]): Unit = {

    using(buildClient) { client =>
      try {
        println("Posting Shots")
        val post = new HttpPost(s"${Creds.getCreds.ShotSite.Href}/addteams")
        val entity = new StringEntity(TeamInfo.toJson(teams))
        post.setEntity(entity)
        val resp = client.execute(post)
        println(resp)
        resp.close()
      } catch {
        case e: Throwable =>
          println(e)
          throw e
      }
    }
  }

}

object Headers {
  val AcceptEncoding: ParameterValue =
    Parameter.newParameterValue("Accept-Encoding", "gzip, deflate, sdch")
  val AcceptLanguage: ParameterValue =
    Parameter.newParameterValue("Accept-Languageg", "n-US,en;q=0.8")
  val UpgradeInsecureRequests: ParameterValue =
    Parameter.newParameterValue("Upgrade-Insecure-Requests", "1")
  val UserAgent: ParameterValue =
    Parameter.newParameterValue("User-Agent", "RRDInsights")
  val Accept: ParameterValue =
    Parameter.newParameterValue("Accept", "application/json")
  val CacheControl: ParameterValue =
    Parameter.newParameterValue("Cache-Control", "max-age=0")
  val Connection: ParameterValue =
    Parameter.newParameterValue("Connection", "keep-alive")
  val Referer: ParameterValue =
    Parameter.newParameterValue("referer", "http://stats.nba.com/scores/")
  val Auth: ParameterValue = Parameter.newParameterValue(
    "Authorization",
    s"Basic ${Creds.getCreds.ShotSite.Auth}")

  val headers: java.util.List[BasicHeader] = Seq(AcceptEncoding,
                                                 AcceptLanguage,
                                                 UpgradeInsecureRequests,
                                                 UserAgent,
                                                 Accept,
                                                 CacheControl,
                                                 Connection,
                                                 Referer,
                                                 Auth)
    .map(v => new BasicHeader(v.name, v.value))
    .asJava
}
