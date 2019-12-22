package com.datatech.wsapi
import akka.stream._
import akka.stream.scaladsl._
import java.nio.file._
import akka.util._
object FileStreaming {

  def fileStreamSource(filePath: String, chunkSize: Int = 1024, dispatcherName: String = ""): Source[ ByteString,Any] = {
    def loadFile  = {
      //   implicit val ec = httpSys.dispatchers.lookup("akka.http.blocking-ops-dispatcher")
      val file = Paths.get(filePath)
      if (dispatcherName != "")
        FileIO.fromPath(file, chunkSize)
          .withAttributes(ActorAttributes.dispatcher(dispatcherName))
      else
        FileIO.fromPath(file, chunkSize)
    }
    loadFile
  }
}