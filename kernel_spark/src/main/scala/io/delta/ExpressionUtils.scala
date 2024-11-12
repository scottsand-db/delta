package io.delta

import io.delta.kernel.expressions.{And => KernelAnd, Column => KernelColumn, Expression => KernelExpression, Literal => KernelLiteral, Or => KernelOr, Predicate => KernelPredicate}
import io.delta.kernel.{types => kerneltypes}
import org.apache.spark.sql.connector.expressions.{Expression => SparkExpression, Literal => SparkLiteral, NamedReference => SparkNamedReference}
import org.apache.spark.sql.connector.expressions.filter.{And => SparkAnd, Or => SparkOr, Predicate => SparkPredicate}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{types => sparktypes}

import scala.collection.JavaConverters._

object ExpressionUtils {
  private val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  //////////////////////
  // Kernel --> Spark //
  //////////////////////

  def convertKtoSPredicate(kernelPredicate: KernelPredicate): Option[SparkPredicate] = {
    val result = convertKtoSExpr(kernelPredicate) match {
      case Some(pred: SparkPredicate) => Some(pred)
      case Some(expr) =>
        throw new IllegalArgumentException(
          s"Expected a SparkPredicate but got ${expr.getClass.getSimpleName}")
      case _ => None
    }

    logger.info(s"convertKtoSPredicate input=$kernelPredicate, result=$result")

    result
  }

  private def convertKtoSExpr(kernelExpression: KernelExpression): Option[SparkExpression] = {
    val result = kernelExpression match {
      case expr: KernelAnd =>
        for {
          left <- convertKtoSPredicate(expr.getLeft)
          right <- convertKtoSPredicate(expr.getRight)
        } yield new SparkAnd(left, right)

      case expr: KernelOr =>
        for {
          left <- convertKtoSPredicate(expr.getLeft)
          right <- convertKtoSPredicate(expr.getRight)
        } yield new SparkOr(left, right)

      case expr: KernelPredicate if expr.getName == "=" =>
        for {
          left <- convertKtoSExpr(expr.getChildren.get(0))
          right <- convertKtoSExpr(expr.getChildren.get(1))
        } yield new SparkPredicate("=", Array(left, right))

      case expr: KernelColumn =>
        Some(new SparkNamedReference {
          override def fieldNames(): Array[String] = expr.getNames

          override def children(): Array[SparkExpression] = {
            expr.getChildren.asScala.map(convertKtoSExpr).flatMap(_.toSeq).toArray
          }

          override def toString: String = s"SparkNamedReferenced(${fieldNames().mkString(".")})"
        })

      case literal: KernelLiteral =>
        literal.getDataType match {
          case _: kerneltypes.BooleanType =>
            Some(convertKtoSLiteral[Boolean](literal, sparktypes.BooleanType))
          case _: kerneltypes.IntegerType =>
            Some(convertKtoSLiteral[Int](literal, sparktypes.IntegerType))
          case _: kerneltypes.LongType =>
            Some(convertKtoSLiteral(literal, sparktypes.LongType))
          case _: kerneltypes.StringType =>
            Some(convertKtoSLiteral[String](literal, sparktypes.StringType))
          case _ => None
        }

      case _ => None
    }

    logger.info(s"convertKtoSExpr: input=$kernelExpression, result=$result")

    result
  }

  private def convertKtoSLiteral[T](
      kernelLiteral: KernelLiteral,
      sparkDataType: sparktypes.DataType): SparkLiteral[T] = {
    new SparkLiteral[T] {
      override def value(): T = kernelLiteral.getValue.asInstanceOf[T]

      override def dataType(): DataType = sparkDataType

      override def toString: String = s"SparkLiteral(value=${value()}, dataType=${dataType()})"
    }
  }

  //////////////////////
  // Spark --> Kernel //
  //////////////////////

  // TODO: perhaps this should NOT return an Option?

  def convertStoKPredicate(sparkPredicate: SparkPredicate): Option[KernelPredicate] = {
    convertStoKExpr(sparkPredicate) match {
      case Some(pred: KernelPredicate) => Some(pred)
      case Some(expr) =>
        throw new IllegalArgumentException(
          s"Expected a KernelPredicate but got ${expr.getClass.getSimpleName}")
      case _ => None
    }
  }

  private def convertStoKExpr(sparkExpression: SparkExpression): Option[KernelExpression] = {
    sparkExpression match {
      case expr: SparkAnd =>
        for {
          left <- convertStoKPredicate(expr.left())
          right <- convertStoKPredicate(expr.right())
        } yield new KernelAnd(left, right)

      case expr: SparkOr =>
        for {
          left <- convertStoKPredicate(expr.left())
          right <- convertStoKPredicate(expr.right())
        } yield new KernelOr(left, right)

      case expr: SparkPredicate if expr.name() == "=" =>
        for {
          left <- convertStoKExpr(expr.children()(0))
          right <- convertStoKExpr(expr.children()(1))
        } yield new KernelPredicate("=", left, right)

      case c: SparkNamedReference =>
        Some(new KernelColumn(c.fieldNames))

      case l: SparkLiteral[Boolean] if l.dataType.isInstanceOf[sparktypes.BooleanType] =>
        Some(KernelLiteral.ofBoolean(l.value))

      case l: SparkLiteral[Int] if l.dataType.isInstanceOf[sparktypes.IntegerType] =>
        Some(KernelLiteral.ofInt(l.value))

      case l: SparkLiteral[Long] if l.dataType.isInstanceOf[sparktypes.LongType] =>
        Some(KernelLiteral.ofLong(l.value))

      case l: SparkLiteral[String] if l.dataType.isInstanceOf[sparktypes.StringType] =>
        Some(KernelLiteral.ofString(l.value))

      case _ => None
    }
  }

}
