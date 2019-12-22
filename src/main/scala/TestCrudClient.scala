import akka.actor._
import akka.http.scaladsl.model.headers._

import scala.concurrent._
import scala.concurrent.duration._
import akka.http.scaladsl.Http
import spray.json.DefaultJsonProtocol
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.marshalling._
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import com.github.tasubo.jurl.URLEncode
import com.datatech.wsapi.MongoModels.Person
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.json4s.jackson
import com.datatech.sdp.mongo.engine.MGOClasses._

trait JsonCodec extends Json4sSupport {
  import org.json4s.DefaultFormats
  import org.json4s.ext.JodaTimeSerializers
  implicit val serilizer = jackson.Serialization
  implicit val formats = DefaultFormats ++ JodaTimeSerializers.all
}
object JsConverters extends JsonCodec

object TestCrudClient {

  type UserInfo = Map[String,Any]
  def main(args: Array[String]): Unit = {
    import JsConverters._

    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext = system.dispatcher



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







    val sort =
      """
        |{userid:-1}
      """.stripMargin

    val getAllRequest = HttpRequest(
      HttpMethods.GET,
      uri = "http://192.168.11.189:50081/public/crud/person?sort="+URLEncode.encode(sort),
    ).addHeader(authentication)
    val futGetAll: Future[HttpResponse] = Http().singleRequest(getAllRequest)
    println(Await.result(futGetAll,2 seconds))
    scala.io.StdIn.readLine()

    var bf =
      """
        |{"userid":"c888"}
      """.stripMargin

    println(URLEncode.encode(bf))

    val delRequest = HttpRequest(
      HttpMethods.DELETE,
      uri = "http://192.168.11.189:50081/public/crud/person?filter="+URLEncode.encode(bf)
    ).addHeader(authentication)
    val futDel: Future[HttpResponse] = Http().singleRequest(delRequest)
    println(Await.result(futDel,2 seconds))
    scala.io.StdIn.readLine()

     bf =
      """
        |{"userid":"c001"}
      """.stripMargin

    val getRequest = HttpRequest(
      HttpMethods.GET,
      uri = "http://192.168.11.189:50081/public/crud/person?filter="+URLEncode.encode(bf),
    ).addHeader(authentication)
    val futGet: Future[HttpResponse] = Http().singleRequest(getRequest)
    println(Await.result(futGet,2 seconds))
    scala.io.StdIn.readLine()

    val tiger = Person("c001","tiger chan",56)
    val john = Person("c002", "johnny dep", 60)
    val peter = Person("c003", "pete brad", 58)
    val susan = Person("c004", "susan boyr", 68,Some(mgoDate(1950,11,5)) )
    val ns = Person("c004", "susan boyr", 68,Some(mgoDate(1950,11,5)) )

    val saveRequest = HttpRequest(
      HttpMethods.POST,
      uri = "http://192.168.11.189:50081/public/crud/person"
    ).addHeader(authentication)
    val futPost: Future[HttpResponse] =
      for {
        reqEntity <- Marshal(peter).to[RequestEntity]
        response <- Http().singleRequest(saveRequest.copy(entity=reqEntity))
      } yield response

    println(Await.result(futPost,2 seconds))
    scala.io.StdIn.readLine()

    var set =
      """
        | {$set:
        |   {
        |    name:"tiger the king",
        |    age:18
        |   }
        | }
      """.stripMargin

    val updateRequest = HttpRequest(
      HttpMethods.PUT,
      uri = "http://192.168.11.189:50081/public/crud/person?filter="+URLEncode.encode(
           bf)+"&set="+URLEncode.encode(set)+"&many=true"
    ).addHeader(authentication)

    val futUpdate: Future[HttpResponse] = Http().singleRequest(updateRequest)
    println(Await.result(futUpdate,2 seconds))
    scala.io.StdIn.readLine()

    val repRequest = HttpRequest(
      HttpMethods.PUT,
      uri = "http://192.168.11.189:50081/public/crud/person?filter="+URLEncode.encode(bf)
    ).addHeader(authentication)
    val futReplace: Future[HttpResponse] =
      for {
        reqEntity <- Marshal(susan).to[RequestEntity]
        response <- Http().singleRequest(updateRequest.copy(entity=reqEntity))
      } yield response

    println(Await.result(futReplace,2 seconds))
    scala.io.StdIn.readLine()

    system.terminate()






  }




}