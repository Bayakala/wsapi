package com.datatech.wsapi
import org.mongodb.scala._
import org.bson.conversions.Bson
import org.mongodb.scala.result._
import com.datatech.sdp.mongo.engine._
import MGOClasses._
import MGOEngine._
import MGOCommands._
import com.datatech.sdp.result.DBOResult.DBOResult
import MongoModels._

object MongoRepo {

   class MongoRepo[R](db:String, coll: String, converter: Option[Document => R])(implicit client: MongoClient) {
    def getAll(next:Option[String],sort:Option[String],fields:Option[String],limit:Option[String],top:Option[String]): DBOResult[Seq[R]] = {
      var res = Seq[ResultOptions]()
      next.foreach {b => res = res :+ ResultOptions(FOD_TYPE.FOD_FILTER,Some(Document(b)))}
      sort.foreach {b => res = res :+ ResultOptions(FOD_TYPE.FOD_SORT,Some(Document(b)))}
      fields.foreach {b => res = res :+ ResultOptions(FOD_TYPE.FOD_PROJECTION,Some(Document(b)))}
      limit.foreach {b => res = res :+ ResultOptions(FOD_TYPE.FOD_LIMIT,Some(Document(b)))}
      top.foreach {b => res = res :+ ResultOptions(FOD_TYPE.FOD_FIRST,Some(Document(b)))}

      val ctxFind = MGOContext(dbName = db,collName=coll)
        .setActionType(MGO_ACTION_TYPE.MGO_QUERY)
        .setCommand(Find(andThen = res))
      mgoQuery[Seq[R]](ctxFind,converter)
    }

     def query(filtr: Bson, next:Option[String],sort:Option[String],fields:Option[String],limit:Option[String],top:Option[String]): DBOResult[Seq[R]] = {
       var res = Seq[ResultOptions]()
       next.foreach {b => res = res :+ ResultOptions(FOD_TYPE.FOD_FILTER,Some(Document(b)))}
       sort.foreach {b => res = res :+ ResultOptions(FOD_TYPE.FOD_SORT,Some(Document(b)))}
       fields.foreach {b => res = res :+ ResultOptions(FOD_TYPE.FOD_PROJECTION,Some(Document(b)))}
       limit.foreach {b => res = res :+ ResultOptions(FOD_TYPE.FOD_LIMIT,Some(Document(b)))}
       top.foreach {b => res = res :+ ResultOptions(FOD_TYPE.FOD_FIRST,Some(Document(b)))}
       val ctxFind = MGOContext(dbName = db,collName=coll)
         .setActionType(MGO_ACTION_TYPE.MGO_QUERY)
         .setCommand(Find(filter = Some(filtr),andThen = res))
       mgoQuery[Seq[R]](ctxFind,converter)
    }
    def getOneDocument(filtr: Bson): DBOResult[Document] = {
          val ctxFind = MGOContext(dbName = db,collName=coll)
         .setActionType(MGO_ACTION_TYPE.MGO_QUERY)
         .setCommand(Find(filter = Some(filtr),firstOnly = true))
       mgoQuery[Document](ctxFind,converter)
    }

    def insert(doc: Document): DBOResult[Completed] = {
      val ctxInsert = MGOContext(dbName = db,collName=coll)
        .setActionType(MGO_ACTION_TYPE.MGO_UPDATE)
        .setCommand(Insert(Seq(doc)))
      mgoUpdate[Completed](ctxInsert)
    }

    def delete(filter: Bson): DBOResult[DeleteResult] = {
      val ctxDelete = MGOContext(dbName = db,collName=coll)
        .setActionType(MGO_ACTION_TYPE.MGO_UPDATE)
        .setCommand(Delete(filter))
      mgoUpdate[DeleteResult](ctxDelete)
    }

    def update(filter: Bson, update: Bson, many: Boolean): DBOResult[UpdateResult] = {
      val ctxUpdate = MGOContext(dbName = db,collName=coll)
        .setActionType(MGO_ACTION_TYPE.MGO_UPDATE)
        .setCommand(Update(filter,update,None,!many))
      mgoUpdate[UpdateResult](ctxUpdate)
    }

    def replace(filter: Bson, row: Document): DBOResult[UpdateResult] = {
       val ctxUpdate = MGOContext(dbName = db,collName=coll)
         .setActionType(MGO_ACTION_TYPE.MGO_UPDATE)
         .setCommand(Replace(filter,row))
       mgoUpdate[UpdateResult](ctxUpdate)
    }

  }

}
