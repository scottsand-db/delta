package io.delta

import io.delta.kernel.data.{ColumnVector => KernelColumnVector}
import io.delta.kernel.types.DataType
import org.apache.spark.sql.catalyst.{InternalRow => SparkInternalRow}

class AbstractVectorWrapper(
    val dataType: DataType,
    protected val bufferReference: Array[SparkInternalRow],
    protected val colIdx: Int
) extends KernelColumnVector {

  override def getDataType: DataType = dataType

  override def getSize: Int = bufferReference.size

  override def close(): Unit = {
    // No-op, override if necessary
  }

  override def isNullAt(rowId: Int): Boolean = {
    checkValidRowId(rowId)
    bufferReference(rowId).isNullAt(colIdx)
  }

  protected def checkValidRowId(rowId: Int): Unit = {
    if (rowId < 0 || rowId >= getSize) {
      throw new IllegalArgumentException(s"invalid row access: $rowId")
    }
  }
}