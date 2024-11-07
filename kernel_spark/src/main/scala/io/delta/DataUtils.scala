package io.delta

import io.delta.kernel.expressions.{Literal => KernelLiteral }
import io.delta.kernel.types.{BooleanType => KernelBooleanType, DataType => KernelDataType, IntegerType => KernelIntegerType, LongType => KernelLongType, StringType => KernelStringType, StructType => KernelStructType}
import org.apache.spark.sql.catalyst.{InternalRow => SparkInternalRow}

object DataUtils {

  def sparkRowToKernelPartitionValues(
      row: SparkInternalRow,
      schema: KernelStructType,
      partitionColNames: Seq[String]): Map[String, KernelLiteral] = {
    partitionColNames.zipWithIndex.map { case (partColName, idx) =>
      val partColDataType = schema.at(idx).getDataType
      val kernelLiteral = sparkRowElementToKernelLiteral(row, partColDataType, idx)
      partColName -> kernelLiteral
    }.toMap
  }

  private def sparkRowElementToKernelLiteral(
      row: SparkInternalRow,
      kernelDataType: KernelDataType,
      index: Int): KernelLiteral = kernelDataType match {
    case _: KernelBooleanType => KernelLiteral.ofBoolean(row.getBoolean(index))
    case _: KernelIntegerType => KernelLiteral.ofInt(row.getInt(index))
    case _: KernelLongType => KernelLiteral.ofLong(row.getLong(index))
    case _: KernelStringType => KernelLiteral.ofString(row.getString(index))
    case _ => throw new IllegalArgumentException(s"unsupported data type $kernelDataType")
  }

}
