package io.delta.data

import io.delta.kernel.data.{ColumnVector => KernelColumnVector, ColumnarBatch => KernelColumnarBatch}
import io.delta.kernel.{types => kerneltypes}
import org.apache.spark.sql.catalyst.{InternalRow => SparkInternalRow}

class SparkRowsArrayToKernelColumnarBatch(
    sparkRows: Array[SparkInternalRow],
    kernelSchema: kerneltypes.StructType)
    extends KernelColumnarBatch {

  private val columnVectorCache: Array[Option[KernelColumnVector]] =
    Array.fill(kernelSchema.length())(None)

  override def getSchema: kerneltypes.StructType = kernelSchema

  override def getSize: Int = sparkRows.length

  override def getColumnVector(ordinal: Int): KernelColumnVector = {
    if (columnVectorCache(ordinal).isEmpty) {
      columnVectorCache(ordinal) = Some(createColumnVector(ordinal))
    }
    columnVectorCache(ordinal).get
  }

  private def createColumnVector(ordinal: Int): KernelColumnVector = {
    val columnType = kernelSchema.at(ordinal).getDataType
    columnType match {
      case _: kerneltypes.IntegerType =>
        new AbstractSparkRowArrayToKernelColumnVectorWrapper(
          columnType,
          sparkRows,
          colIdx = ordinal) {
          override def getInt(rowId: Int): Int = {
            checkValidRowId(rowId)
            bufferReference(rowId).getInt(colIdx)
          }
        }

      case _: kerneltypes.StringType =>
        new AbstractSparkRowArrayToKernelColumnVectorWrapper(
          columnType,
          sparkRows,
          colIdx = ordinal) {
          override def getString(rowId: Int): String = {
            checkValidRowId(rowId)
            bufferReference(rowId).getUTF8String(colIdx).toString
          }
        }

      case _: kerneltypes.BooleanType =>
        new AbstractSparkRowArrayToKernelColumnVectorWrapper(
          columnType,
          sparkRows,
          colIdx = ordinal) {
          override def getBoolean(rowId: Int): Boolean = {
            checkValidRowId(rowId)
            bufferReference(rowId).getBoolean(colIdx)
          }
        }

      case _: kerneltypes.LongType =>
        new AbstractSparkRowArrayToKernelColumnVectorWrapper(
          columnType,
          sparkRows,
          colIdx = ordinal) {
          override def getLong(rowId: Int): Long = {
            checkValidRowId(rowId)
            bufferReference(rowId).getLong(colIdx)
          }
        }

      case unsupported =>
        throw new UnsupportedOperationException(s"Unsupported data type: $unsupported")
    }
  }
}
