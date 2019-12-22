package com.datatech.wsapi
import akka.actor._
import akka.stream._
import akka.stream.scaladsl._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model._
import akka.http.scaladsl.coding.Gzip
import java.nio.file._
import FileStreaming._
import AuthBase._

case class FileRoute(jwt: String)(implicit auth: AuthBase, sys: ActorSystem) {

  val destPath = "/users/tiger-macpro/cert4/meme.jpg"
  implicit val mat = ActorMaterializer()
  val route = pathPrefix("file") {
    val privatePath = auth.tempDirFromJwt(jwt)
    if (privatePath.length == 0)
      complete(StatusCodes.NotFound)

    (get & path(Remaining)) { filename =>
      withoutSizeLimit {
        encodeResponseWith(Gzip) {
          complete(
            HttpEntity(
              ContentTypes.`application/octet-stream`,
              fileStreamSource(privatePath+"/download/"+filename, 1024))
          )
        }
      }
    } ~
      (post &  parameters('filename)) { filename =>
        withoutSizeLimit {
          decodeRequest {
            extractDataBytes { bytes =>
              val fut = bytes.runWith(FileIO.toPath(Paths.get(privatePath+"/upload/"+filename)))
              onComplete(fut) { _ => complete(StatusCodes.OK)}
            }
          }
        }

      }

  }
}