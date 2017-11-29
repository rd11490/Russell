package com.rrdinsights.russell.utils

object MapJoin {

  def join[K1, V1, V2](left: Map[K1, V1], right: Map[K1, V2]): Seq[(V1, V2)] = {
    left.flatMap(v => right.get(v._1).map(c => (v._2, c))).toSeq
  }

  def leftOuterJoin[K1, V1, V2](left: Map[K1, V1], right: Map[K1, V2]): Seq[(V1, Option[V2])] = {
    left.map(v => (v._2, right.get(v._1))).toSeq
  }
}
