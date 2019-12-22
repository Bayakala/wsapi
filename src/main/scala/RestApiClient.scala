import akka.actor._
import akka.stream._
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers._
import scala.concurrent._
import akka.http.scaladsl.model._
import pdi.jwt._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.util._
import scala.concurrent.duration._



object RestApiClient  {
  type UserInfo = Map[String,Any]
  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext = system.dispatcher

    val helloRequest = HttpRequest(uri = "http://192.168.0.189:50081/")

    val authorization = headers.Authorization(BasicHttpCredentials("johnny", "p4ssw0rd"))
    val authRequest = HttpRequest(
      HttpMethods.POST,
      uri = "http://192.168.0.189:50081/auth",
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

    val parts = Jwt.decodeRawAll(jstr, "OpenSesame", Seq(JwtAlgorithm.HS256)) match {
      case Failure(exception) => println(s"Error: ${exception.getMessage}")
      case Success(value) =>
        println(((parse(value._2).asInstanceOf[JObject]) \ "userinfo").values.asInstanceOf[UserInfo])
    }

    scala.io.StdIn.readLine()

    val authentication = headers.Authorization(OAuth2BearerToken(jstr))
    val apiRequest = HttpRequest(
      HttpMethods.GET,
      uri = "http://192.168.0.189:50081/api/hello",
    ).addHeader(authentication)

    val futAuth: Future[HttpResponse] = Http().singleRequest(apiRequest)

    println(Await.result(futAuth,2 seconds))


    scala.io.StdIn.readLine()
    system.terminate()
  }

}
