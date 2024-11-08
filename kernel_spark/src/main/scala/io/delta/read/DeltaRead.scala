package io.delta.read

import io.delta.data.KernelRowToSparkRowWrapper
import io.delta.kernel.{Scan => KernelScan}
import io.delta.kernel.defaults.internal.json.JsonUtils
import io.delta.kernel.internal.InternalScanFileUtils
import io.delta.kernel.internal.data.ScanStateRow
import io.delta.kernel.internal.util.Utils
import io.delta.kernel.utils.CloseableIterator
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.vectorized.ColumnarBatch

/** Serialized and sent from the Driver to the Executors */
class DeltaReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    require(partition.isInstanceOf[DeltaInputPartition])

    new DeltaPartitionReaderOfRows(partition.asInstanceOf[DeltaInputPartition])
  }

  // TODO
  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] =
    super.createColumnarReader(partition)

  // TODO
  override def supportColumnarReads(partition: InputPartition): Boolean =
    super.supportColumnarReads(partition)
}

///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////

/** Created on the executor. */
class DeltaPartitionReaderOfRows(deltaInputPartition: DeltaInputPartition)
    extends PartitionReader[InternalRow] {
  val engine = io.delta.kernel.defaults.engine.DefaultEngine.create(new Configuration())

  val scanFileRow = JsonUtils.rowFromJson(
    deltaInputPartition.serializedScanFileRow,
    InternalScanFileUtils.SCAN_FILE_SCHEMA)

  val addFileStatus = InternalScanFileUtils.getAddFileStatus(scanFileRow)

  val scanStateRow =
    JsonUtils.rowFromJson(deltaInputPartition.serializedScanState, ScanStateRow.SCHEMA)

  private val physicalRowDataIter = engine.getParquetHandler
    .readParquetFiles(
      Utils.singletonCloseableIterator(addFileStatus),
      ScanStateRow.getPhysicalDataReadSchema(engine, scanStateRow),
      java.util.Optional.empty() /* predicate */ )

  private val logicalRowDataIter =
    KernelScan.transformPhysicalData(engine, scanStateRow, scanFileRow, physicalRowDataIter)

  private var rowIter: CloseableIterator[io.delta.kernel.data.Row] = null
  private var curr: io.delta.kernel.data.Row = null
  private var closed = false

  override def close(): Unit = {
    logicalRowDataIter.close()
    if (rowIter != null) {
      rowIter.close()
    }
    closed = true
  }

  override def next(): Boolean = {
    if (!closed) {
      // Check if there are remaining rows in the current row iterator
      if (rowIter != null && rowIter.hasNext) {
        curr = rowIter.next()
        true
      }
      // If current batch is exhausted, fetch the next batch and reset the row iterator
      else if (logicalRowDataIter.hasNext) {
        rowIter = logicalRowDataIter.next().getRows
        next() // Recursively call next to process the new batch
      } else {
        false
      }
    } else {
      false
    }
  }

  override def get(): InternalRow = {
    if (curr == null) {
      throw new NoSuchElementException("No current row available; call next() first.")
    }
    new KernelRowToSparkRowWrapper(curr)
  }
}
