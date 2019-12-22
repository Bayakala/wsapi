package com.datatech.wsapi
import scala.concurrent._

trait RepoBase[M] {
  def getById(id: Long) : Future[Option[M]]
  def getAll : Future[Seq[M]]
  def filter(expr: M => Boolean): Future[Seq[M]]
  def save(row: M) : Future[AnyRef]
  def deleteById(id: Long) : Future[Int]
  def updateById(id: Long, row: M) : Future[Int]
}
