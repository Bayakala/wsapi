package com.datatech.wsapi
import akka.http.scaladsl.server.Directives
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.json4s.jackson
abstract class RouteBase[M](val pathName: String, repository: RepoBase[M])(
  implicit m: Manifest[M]) extends Directives with JsonConverter {

  val route = path(pathName) {
    get {
      complete(futureToJson(repository.getAll))
    } ~ post {
      entity(as[String]) { json =>
        val extractedEntity = fromJson[M](json)
        complete(futureToJson(repository.save(extractedEntity)))
      }
    }
  } ~ path(pathName / LongNumber) { id =>
    get {
      complete(futureToJson(repository.getById(id)))
    } ~ put {
      entity(as[String]) { json =>
        val extractedEntity = fromJson[M](json)
        complete(futureToJsonAny(repository.updateById(id, extractedEntity)))
      }
    } ~ delete {
      complete(futureToJsonAny(repository.deleteById(id)))
    }
  }
}