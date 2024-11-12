package io.delta.data

import io.delta.SchemaUtils
import io.delta.kernel.data.{ColumnVector => KernelColumnVector, FilteredColumnarBatch => KernelFilteredColumnarBatch}
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.vectorized.{ColumnarArray, ColumnarMap, ColumnVector => SparkColumnVector, ColumnarBatch => SparkColumnarBatch}
import org.apache.spark.unsafe.types.UTF8String

class KernelColumnarBatchToSparkColumnarBatchWrapper(
    columns: Array[SparkColumnVector],
    numRows: Int)
    extends SparkColumnarBatch(columns, numRows) {}

object KernelColumnarBatchToSparkColumnarBatchWrapper {
  private val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  def apply(kernelFilteredColumnarBatch: KernelFilteredColumnarBatch)
      : KernelColumnarBatchToSparkColumnarBatchWrapper = {
    val kernelColumnarBatch = kernelFilteredColumnarBatch.getData
    val numColumns = kernelColumnarBatch.getSchema.length()
    val numRows = kernelColumnarBatch.getSize

    logger.info(s"kernelColumnarBatch: numRows $numRows, numColumns: $numColumns, " +
      s"getSchema ${kernelColumnarBatch.getSchema}")

    val columns: Array[SparkColumnVector] = (0 until numColumns).map { i =>
      logger.info(s"Creating SparkColumnVector for column $i: " +
        s"${kernelColumnarBatch.getColumnVector(i).getDataType}")
      new KernelColumnVectorToSparkColumnVectorWrapper(kernelColumnarBatch.getColumnVector(i))
    }.toArray

    new KernelColumnarBatchToSparkColumnarBatchWrapper(columns, kernelColumnarBatch.getSize)
  }
}

class KernelColumnVectorToSparkColumnVectorWrapper(kernelColumnVector: KernelColumnVector)
    extends SparkColumnVector(
      SchemaUtils.convertKernelDataTypeToSparkDataType(kernelColumnVector.getDataType)) {

  override def close(): Unit = kernelColumnVector.close()

  override def hasNull: Boolean = true

  override def numNulls(): Int = 0

  override def isNullAt(rowId: Int): Boolean = kernelColumnVector.isNullAt(rowId)

  override def getBoolean(rowId: Int): Boolean = kernelColumnVector.getBoolean(rowId)

  override def getByte(rowId: Int): Byte = kernelColumnVector.getByte(rowId)

  override def getShort(rowId: Int): Short = kernelColumnVector.getShort(rowId)

  override def getInt(rowId: Int): Int = kernelColumnVector.getInt(rowId)

  override def getLong(rowId: Int): Long = kernelColumnVector.getLong(rowId)

  override def getFloat(rowId: Int): Float = kernelColumnVector.getFloat(rowId)

  override def getDouble(rowId: Int): Double = kernelColumnVector.getDouble(rowId)

  override def getUTF8String(rowId: Int): UTF8String =
    UTF8String.fromString(kernelColumnVector.getString(rowId))

  override def getBinary(rowId: Int): Array[Byte] = kernelColumnVector.getBinary(rowId)

  override def getArray(rowId: Int): ColumnarArray = throw new UnsupportedOperationException(
    "getArray is not supported")

  override def getMap(rowId: Int): ColumnarMap = throw new UnsupportedOperationException(
    "getMap is not supported")

  override def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal =
    throw new UnsupportedOperationException("getDecimal is not supported")

  override def getChild(ordinal: Int): SparkColumnVector =
    throw new UnsupportedOperationException("getChild is not supported")
}
