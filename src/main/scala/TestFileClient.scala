import akka.stream._
import java.nio.file._
import java.io._
import akka.http.scaladsl.model.headers._
import scala.concurrent._
import com.datatech.wsapi.FileStreaming._
import scala.concurrent.duration._
import akka.actor.ActorSystem
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model._
import akka.http.scaladsl.Http
import akka.stream.scaladsl.{FileIO, Source}
import scala.util._


case class FileUtil(implicit sys: ActorSystem) {
  import sys.dispatcher
  implicit val mat = ActorMaterializer()
  def createEntity(file: File): RequestEntity = {
    require(file.exists())
    val formData =
      Multipart.FormData(
        Source.single(
          Multipart.FormData.BodyPart(
            "test",
            HttpEntity(MediaTypes.`application/octet-stream`, file.length(), FileIO.fromPath(file.toPath, chunkSize = 100000)), // the chunk size here is currently critical for performance
            Map("filename" -> file.getName))))
    Await.result(Marshal(formData).to[RequestEntity], 3 seconds)
  }

  def uploadFile(request: HttpRequest, dataEntity: RequestEntity) = {
    implicit val mat = ActorMaterializer()
    import sys.dispatcher
    val futResp = Http(sys).singleRequest(
      //   Gzip.encodeMessage(
      request.copy(entity = dataEntity)   //.addHeader(`Content-Encoding`(HttpEncodings.gzip))
      //   )
    )
    futResp
      .andThen {
        case Success(r@HttpResponse(StatusCodes.OK, _, entity, _)) =>
          entity.dataBytes.map(_.utf8String).runForeach(println)
        case Success(r@HttpResponse(code, _, _, _)) =>
          println(s"Upload request failed, response code: $code")
          r.discardEntityBytes()
        case Success(_) => println("Unable to Upload file!")
        case Failure(err) => println(s"Upload failed: ${err.getMessage}")
      }
  }

  def downloadFileTo(request: HttpRequest, destPath: String) = {
    //  val req = request.addHeader(`Content-Encoding`(HttpEncodings.gzip))
    val futResp = Http(sys).singleRequest(request)  //.map(Gzip.decodeMessage(_))
    futResp
      .andThen {
        case Success(r@HttpResponse(StatusCodes.OK, _, entity, _)) =>
          entity.withoutSizeLimit().dataBytes.runWith(FileIO.toPath(Paths.get(destPath)))
            .onComplete { case _ => println(s"Download file saved to: $destPath") }
        case Success(r@HttpResponse(code, _, _, _)) =>
          println(s"Download request failed, response code: $code")
          r.discardEntityBytes()
        case Success(_) => println("Unable to download file!")
        case Failure(err) => println(s"Download failed: ${err.getMessage}")
      }

  }

}

object TestFileClient  {
  type UserInfo = Map[String,Any]
  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext = system.dispatcher

    val helloRequest = HttpRequest(uri = "http://192.168.11.189:50081/")

    val authorization = headers.Authorization(BasicHttpCredentials("johnny", "p4ssw0rd"))
    val authRequest = HttpRequest(
      HttpMethods.POST,
      uri = "http://192.168.11.189:50081/auth",
      headers = List(authorization)
    )


    val futToken: Future[HttpResponse] = Http().singleRequest(authRequest)

    val respToken = for {
      resp <- futToken
      jstr <- resp.entity.dataBytes.runFold("") {(s,b) => s + b.utf8String}
    } yield jstr

    val jstr =  Await.result[String](respToken,2 seconds)
    println(jstr)

    scala.io.StdIn.readLine()

    val authentication = headers.Authorization(OAuth2BearerToken(jstr))

    val entity = HttpEntity(
      ContentTypes.`application/octet-stream`,
      fileStreamSource("/Users/tiger-macpro/cert3/ctiger.jpg",1024)
    )
    //
    val chunked = HttpEntity.Chunked.fromData(
      ContentTypes.`application/octet-stream`,
      fileStreamSource("/Users/tiger-macpro/cert3/ctiger.jpg",1024)
    )

    val multipart = FileUtil().createEntity(new File("/Users/tiger-macpro/cert3/ctiger.jpg"))

    val uploadRequest = HttpRequest(
      HttpMethods.POST,
      uri = "http://192.168.11.189:50081/public/crud/photo/blob?id=tiger.jpg",
    ).addHeader(authentication)

    //upload file
    Await.ready(FileUtil().uploadFile(uploadRequest,entity),2 seconds)
    //Await.ready(FileUtil().uploadFile(uploadRequest,chunked),2 seconds)
    //Await.ready(FileUtil().uploadFile(uploadRequest,multipart),2 seconds)


    val dlRequest = HttpRequest(
      HttpMethods.GET,
      uri = "http://192.168.11.189:50081/api/file/mypic.jpg",
    ).addHeader(authentication)

 //   FileUtil().downloadFileTo(dlRequest, "/users/tiger-macpro/cert3/mypic.jpg")

    scala.io.StdIn.readLine()
    system.terminate()
  }

}