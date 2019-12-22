package com.datatech.wsapi
import org.mongodb.scala._
import com.datatech.sdp.mongo.engine._
import MGOClasses._

object MongoModels {

  case class Person(
                   userid: String = "",
                   name: String = "",
                   age: Int = 18,
                   dob: Option[MGODate] = None,
                   address: Option[String] = None
                   ) extends ModelBase[Person,Document] {
    import org.mongodb.scala.bson._

    override def to: Document = {
      var doc = Document(
      "userid" -> this.userid,
      "name" -> this.name,
      "age" -> this.age)


      if (this.dob != None)
        doc = doc + ("dob" -> this.dob.get)

      if (this.address != None)
        doc = doc + ("address" -> this.address.getOrElse(""))

      doc
    }

  }
  object Person {
    val fromDocument: Document => Person = doc => {
      val keyset = doc.keySet
      Person(
        userid = doc.getString("userid"),
        name = doc.getString("name"),
        age = doc.getInteger("age"),

        dob =  {if (keyset.contains("dob"))
          Some(doc.getDate("dob"))
        else None },

        address =  mgoGetStringOrNone(doc,"address")
      )
    }
  }

  case class Photo (
                     id: String,
                     photo: Option[MGOBlob]
                   ) extends ModelBase[Photo,Document] {
    override def to: Document = {
      var doc = Document("id" -> this.id)
      if (photo != None)
        doc = doc + ("photo" -> this.photo)
      doc
    }
  }

  object Photo {
    def fromDocument: Document => Photo = doc => {
      val keyset = doc.keySet
      Photo(
        id = doc.getString("id"),
        photo = mgoGetBlobOrNone(doc, "photo")
      )
    }
  }


}

