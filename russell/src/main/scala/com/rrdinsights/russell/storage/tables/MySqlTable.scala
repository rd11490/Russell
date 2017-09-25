package com.rrdinsights.russell.storage.tables

import com.rrdinsights.russell.storage.{MySqlUtils, SqlTypeHolder}

import scala.reflect.runtime.universe._


trait MySqlTable  {
  def name: String

  def columns: Seq[SqlTypeHolder]
}

private[storage] object MySqlTable {

  private final case class TableImpl[T: TypeTag](_name: String) extends MySqlTable {
    override def name: String = _name

    override def columns: IndexedSeq[SqlTypeHolder] = MySqlUtils.getFields[T]
  }

  def apply[T: TypeTag](name: String): MySqlTable =
    TableImpl[T](name)
}