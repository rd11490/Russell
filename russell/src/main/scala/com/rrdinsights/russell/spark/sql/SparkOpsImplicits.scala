package com.rrdinsights.russell.spark.sql

import org.apache.spark.sql._

object SparkOpsImplicits {

  final implicit class DatasetOps[L](leftDs: Dataset[L]) {

    def apply(column: Column): Column = leftDs(column.toString())

    /** DataFrame Joins */
    def joinUsing(rightDs: Dataset[_], joinColumn: Column, moreColumns: Column*): DataFrame =
    joinUsing(rightDs, JoinType.Inner, joinColumn, moreColumns: _*)


    def leftOuterJoinUsing(rightDs: Dataset[_], joinColumn: Column, moreColumns: Column*): DataFrame =
      joinUsing(rightDs, JoinType.LeftOuter, joinColumn, moreColumns: _*)

    def rightOuterJoinUsing(rightDs: Dataset[_], joinColumn: Column, moreColumns: Column*): DataFrame =
      joinUsing(rightDs, JoinType.RightOuter, joinColumn, moreColumns: _*)

    def fullOuterJoinUsing(rightDs: Dataset[_], joinColumn: Column, moreColumns: Column*): DataFrame =
      joinUsing(rightDs, JoinType.FullOuter, joinColumn, moreColumns: _*)

    def joinUsing(rightDs: Dataset[_], joinType: JoinType, joinColumn: Column, moreColumns: Column*): DataFrame = {
      val joinColumns = (joinColumn +: moreColumns).map(_.toString())
      leftDs.join(rightDs, joinColumns, joinType.name)
    }

    def leftOuterJoin(rightDs: Dataset[_], joinExprs: Column): DataFrame =
      leftDs.join(rightDs, joinExprs, JoinType.LeftOuter.name)

    def rightOuterJoin(rightDs: Dataset[_], joinExprs: Column): DataFrame =
      leftDs.join(rightDs, joinExprs, JoinType.RightOuter.name)

    def fullOuterJoin(rightDs: Dataset[_], joinExprs: Column): DataFrame =
      leftDs.join(rightDs, joinExprs, JoinType.FullOuter.name)


    /** Dataset Joins On Column (Can return null) */
    def joinWithUsingDS[R](rightDs: Dataset[R], joinColumn: Column, moreColumns: Column*): Dataset[(L, R)] =
    joinUsingRawDS(rightDs, JoinType.Inner, joinColumn, moreColumns: _*)

    def leftOuterJoinWithUsingDS[R](rightDs: Dataset[R], joinColumn: Column, moreColumns: Column*): Dataset[(L, R)] =
      joinUsingRawDS(rightDs, JoinType.LeftOuter, joinColumn, moreColumns: _*)

    def rightOuterJoinWithUsingDS[R](rightDs: Dataset[R], joinColumn: Column, moreColumns: Column*): Dataset[(L, R)] =
      joinUsingRawDS(rightDs, JoinType.RightOuter, joinColumn, moreColumns: _*)

    def fullOuterJoinUsingRawDS[R](rightDs: Dataset[R], joinColumn: Column, moreColumns: Column*): Dataset[(L, R)] =
      joinUsingRawDS(rightDs, JoinType.FullOuter, joinColumn, moreColumns: _*)

    def joinUsingRawDS[R](rightDs: Dataset[R], joinType: JoinType, joinColumn: Column, moreColumns: Column*): Dataset[(L, R)] = {
      val joinColumns = (joinColumn +: moreColumns).map(c => leftDs(c) === rightDs(c)).reduce(_ && _)
      leftDs.joinWith(rightDs, joinColumns, joinType.name)
    }

    /** Dataset Joins with expression (Can return null) */

    def leftOuterJoinWithDS[R](rightDs: Dataset[R], joinExp: Column): Dataset[(L, R)] =
    joinWithDS(rightDs, joinExp, JoinType.LeftOuter)

    def rightOuterJoinWithDS[R](rightDs: Dataset[R], joinExp: Column): Dataset[(L, R)] =
      joinWithDS(rightDs, joinExp, JoinType.RightOuter)

    def fullOuterJoinWithDS[R](rightDs: Dataset[R], joinExp: Column): Dataset[(L, R)] =
      joinWithDS(rightDs, joinExp, JoinType.FullOuter)

    def joinWithDS[R](rightDs: Dataset[R], joinExp: Column, joinType: JoinType): Dataset[(L, R)] =
      leftDs.joinWith(rightDs, joinExp, joinType.name)

    /** Dataset and merge joins */
    def joinDS[R, J](rightDS: Dataset[R], mergeFunc: (L, R) => J, joinExpr: Column)
                    (implicit spark: SparkSession, encoder: Encoder[J]): Dataset[J] =
    leftDs.joinWithDS(rightDS, joinExpr, JoinType.Inner).map(v => mergeFunc.tupled(v))

    def leftOuterJoinDS[R, J](rightDS: Dataset[R], mergeFunc: (L, R) => Option[J], joinExpr: Column)
                             (implicit spark: SparkSession, encoder: Encoder[J]): Dataset[J] =
      leftDs.joinWithDS(rightDS, joinExpr, JoinType.LeftOuter).flatMap(v => mergeFunc.tupled(v))

    def rightOuterJoinDS[R, J](rightDS: Dataset[R], mergeFunc: (L, R) => Option[J], joinExpr: Column)
                              (implicit spark: SparkSession, encoder: Encoder[J]): Dataset[J] =
      leftDs.joinWithDS(rightDS, joinExpr, JoinType.RightOuter).flatMap(v => mergeFunc.tupled(v))

    def fullOuterJoinDS[R, J](rightDS: Dataset[R], mergeFunc: (L, R) => Option[J], joinExpr: Column)
                             (implicit spark: SparkSession, encoder: Encoder[J]): Dataset[J] =
      leftDs.joinWithDS(rightDS, joinExpr, JoinType.FullOuter).flatMap(v => mergeFunc.tupled(v))

    /** Dataset and merge join on columns */
    def joinUsingDS[R, J](rightDS: Dataset[R], mergeFunc: (L, R) => J, joinColumn: Column, moreColumns: Column*)
                         (implicit spark: SparkSession, encoder: Encoder[J]): Dataset[J] =
    leftDs.joinUsingRawDS(rightDS, JoinType.Inner, joinColumn, moreColumns: _*).map(v => mergeFunc.tupled(v))

    def leftOuterJoinUsingDS[R, J](rightDS: Dataset[R], mergeFunc: (L, R) => Option[J], joinColumn: Column, moreColumns: Column*)
                                  (implicit spark: SparkSession, encoder: Encoder[J]): Dataset[J] =
      leftDs.leftOuterJoinWithUsingDS(rightDS, joinColumn, moreColumns: _*).flatMap(v => mergeFunc.tupled(v))

    def rightOuterJoinUsingDS[R, J](rightDS: Dataset[R], mergeFunc: (L, R) => Option[J], joinColumn: Column, moreColumns: Column*)
                                   (implicit spark: SparkSession, encoder: Encoder[J]): Dataset[J] =
      leftDs.rightOuterJoinWithUsingDS(rightDS, joinColumn, moreColumns: _*).flatMap(v => mergeFunc.tupled(v))

    def fullOuterJoinUsingDS[R, J](rightDS: Dataset[R], mergeFunc: (L, R) => Option[J], joinColumn: Column, moreColumns: Column*)
                                  (implicit spark: SparkSession, encoder: Encoder[J]): Dataset[J] =
      leftDs.fullOuterJoinUsingRawDS(rightDS, joinColumn, moreColumns: _*).flatMap(v => mergeFunc.tupled(v))

  }

}