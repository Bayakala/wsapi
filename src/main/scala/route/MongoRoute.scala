package com.datatech.wsapi
import akka.http.scaladsl.server.Directives
import scala.util._
import org.mongodb.scala._
import com.datatech.sdp.file.Streaming._
import org.mongodb.scala.result._
import MongoRepo._
import akka.stream.ActorMaterializer
import com.datatech.sdp.result.DBOResult._
import org.mongodb.scala.model.Filters._
import com.datatech.sdp.mongo.engine.MGOClasses._
import monix.execution.CancelableFuture
import akka.util._
import akka.http.scaladsl.model._


import akka.http.scaladsl.coding.Gzip
object MongoRoute {
  class MongoRoute[M <: ModelBase[M, Document]](val pathName: String)(repository: MongoRepo[M])(
    implicit c: MongoClient, m: Manifest[M], mat: ActorMaterializer) extends Directives with JsonConverter {
    import monix.execution.Scheduler.Implicits.global
    var dbor: DBOResult[Seq[M]] = _
    var dbou: DBOResult[UpdateResult] = _
    val route = pathPrefix(pathName) {
      pathPrefix("blob") {
        (get & path(Remaining)) { id =>
          val filtr = equal("id", id)
          val futOptPic: CancelableFuture[Option[MGOBlob]] = repository.getOneDocument(filtr).value.value.runToFuture.map {
            eodoc =>
              eodoc match {
                case Right(odoc) => odoc match {
                  case Some(doc) =>
                    if (doc == null) None
                    else mgoGetBlobOrNone(doc, "photo")
                  case None => None
                }
                case Left(_) => None
              }
          }
          onComplete(futOptPic) {
            case Success(optBlob) => optBlob match {
              case Some(blob) =>
                withoutSizeLimit {
                  encodeResponseWith(Gzip) {
                    complete(
                      HttpEntity(
                        ContentTypes.`application/octet-stream`,
                        ByteArrayToSource(blob.getData))
                    )
                  }
                }
              case None => complete(StatusCodes.NotFound)
            }
            case Failure(err) => complete(err)
          }
        } ~
        (post &  parameter('id)) { id =>
          withoutSizeLimit {
            decodeRequest {
              extractDataBytes { bytes =>
                val fut = bytes.runFold(ByteString()) { case (hd, bs) =>
                  hd ++ bs
                }
                onComplete(fut) {
                  case Success(b) =>
                    val doc = Document("id" -> id, "photo" -> b.toArray)
                    val futmsg = repository.insert(doc).value.value.runToFuture.map {
                      eoc =>
                        eoc match {
                          case Right(oc) => oc match {
                            case Some(c) => c.toString()
                            case None => "insert may not complete!"
                          }
                          case Left(err) => err.getMessage
                        }
                    }
                    complete(futmsg)
                  case Failure(err) => complete(err)
                }
              }
            }
          }
        }
      } ~
      (get & parameters('filter.?,'fields.?,'sort.?,'limit.?, 'top.?, 'next.?)) {
        (filter,fields,sort,limit,top,next) => {
        dbor = {
          filter match {
            case Some(fltr) => repository.query(Document(fltr),next,sort,fields,limit,top)
            case None => repository.getAll(next,sort,fields,limit,top)
          }
        }
        val futRows = dbor.value.value.runToFuture.map {
          eolr =>
            eolr match {
              case Right(olr) => olr match {
                case Some(lr) => lr
                case None => Seq[M]()
              }
              case Left(_) => Seq[M]()
            }
        }
        complete(futureToJson(futRows))
       }
      } ~ post {
        entity(as[String]) { json =>
          val extractedEntity: M = fromJson[M](json)
          val doc: Document = extractedEntity.to
          val futmsg = repository.insert(doc).value.value.runToFuture.map {
            eoc =>
              eoc match {
                case Right(oc) => oc match {
                  case Some(c) => c.toString()
                  case None => "insert may not complete!"
                }
                case Left(err) => err.getMessage
              }
          }

          complete(futmsg)
        }
      } ~ (put & parameter('filter,'set.?, 'many.as[Boolean].?)) { (filter, set, many) =>
        val bson = Document(filter)
        if (set == None) {
          entity(as[String]) { json =>
            val extractedEntity: M = fromJson[M](json)
            val doc: Document = extractedEntity.to
            val futmsg = repository.replace(bson, doc).value.value.runToFuture.map {
              eoc =>
                eoc match {
                  case Right(oc) => oc match {
                    case Some(d) => s"${d.getMatchedCount} matched rows, ${d.getModifiedCount} rows updated."
                    case None => "update may not complete!"
                  }
                  case Left(err) => err.getMessage
                }
            }
            complete(futureToJson(futmsg))
          }
        } else {
          set match {
            case Some(u) =>
              val ubson = Document(u)
              dbou = repository.update(bson, ubson, many.getOrElse(true))
            case None =>
              dbou = Left(new IllegalArgumentException("missing set statement for update!"))
          }
          val futmsg = dbou.value.value.runToFuture.map {
            eoc =>
              eoc match {
                case Right(oc) => oc match {
                  case Some(d) => s"${d.getMatchedCount} matched rows, ${d.getModifiedCount} rows updated."
                  case None => "update may not complete!"
                }
                case Left(err) => err.getMessage
              }
          }
          complete(futureToJson(futmsg))
        }
      } ~ (delete & parameters('filter, 'many.as[Boolean].?)) { (filter,many) =>
        val bson = Document(filter)
        val futmsg = repository.delete(bson).value.value.runToFuture.map {
          eoc =>
            eoc match {
              case Right(oc) => oc match {
                case Some(d) => s"${d.getDeletedCount} rows deleted."
                case None => "delete may not complete!"
              }
              case Left(err) => err.getMessage
            }
        }
        complete(futureToJson(futmsg))
      }
    }
  }

}


/*
curl -i -X GET http://rest-api.io/items
curl -i -X GET http://rest-api.io/items/5069b47aa892630aae059584
curl -i -X DELETE http://rest-api.io/items/5069b47aa892630aae059584
curl -i -X POST -H 'Content-Type: application/json' -d '{"name": "New item", "year": "2009"}' http://rest-api.io/items
curl -i -X PUT -H 'Content-Type: application/json' -d '{"name": "Updated item", "year": "2010"}' http://rest-api.io/items/5069b47aa892630aae059584
 */