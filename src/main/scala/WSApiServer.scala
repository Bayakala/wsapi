package com.datatech.wsapi

import akka.actor._
import akka.stream._
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import pdi.jwt._
import AuthBase._
import MockUserAuthService._
import org.mongodb.scala._

import scala.collection.JavaConverters._
import MongoModels._
import MongoRepo._
import MongoRoute._


object WSApiServer extends App {


  implicit val httpSys = ActorSystem("httpSystem")
  implicit val httpMat = ActorMaterializer()
  implicit val httpEC = httpSys.dispatcher

  val settings: MongoClientSettings = MongoClientSettings.builder()
    .applyToClusterSettings(b => b.hosts(List(new ServerAddress("localhost")).asJava))
    .build()
  implicit val client: MongoClient = MongoClient(settings)
  implicit val personDao = new MongoRepo[Person]("testdb","person", Some(Person.fromDocument))
  implicit val picDao = new MongoRepo[Photo]("testdb","photo", None)

  implicit val authenticator = new AuthBase()
    .withAlgorithm(JwtAlgorithm.HS256)
    .withSecretKey("OpenSesame")
    .withUserFunc(getValidUser)

  val route =
    path("auth") {
      authenticateBasic(realm = "auth", authenticator.getUserInfo) { userinfo =>
        post { complete(authenticator.issueJwt(userinfo))}
      }
    } ~
      pathPrefix("private") {
        authenticateOAuth2(realm = "private", authenticator.authenticateToken) { validToken =>
          FileRoute(validToken)
            .route
          // ~ ...
        }
      } ~
        pathPrefix("public") {
          (pathPrefix("crud")) {
            new MongoRoute[Person]("person")(personDao)
              .route ~
              new MongoRoute[Photo]("photo")(picDao)
                .route
          }
        }

  val (port, host) = (50081,"192.168.11.189")

  val bindingFuture = Http().bindAndHandle(route,host,port)

  println(s"Server running at $host $port. Press any key to exit ...")

  scala.io.StdIn.readLine()


  bindingFuture.flatMap(_.unbind())
    .onComplete(_ => httpSys.terminate())


}