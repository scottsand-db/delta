package io.delta

import io.delta.kernel.{Scan => KernelScan, Table => KernelTable}
import io.delta.kernel.engine.{Engine => KernelEngine}
import io.delta.read.{DeltaScan, DeltaScanBuilder}
import io.delta.write.{DeltaOfRowsWriteBuilder, SparkWriteBuilder}
import org.apache.spark.sql.connector
import org.apache.spark.sql.connector.expressions.{Expressions, NamedReference}
import org.apache.spark.sql.connector.read.{ScanBuilder, Scan => SparkScan}
import org.apache.spark.sql.connector.write.{DeltaWriteBuilder, LogicalWriteInfo, RowLevelOperation, RowLevelOperationBuilder, RowLevelOperationInfo, SupportsDelta, WriteBuilder}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

sealed trait RowLevelOperationMode {
  def description: String
}

case object CopyOnWrite extends RowLevelOperationMode {
  val description: String = "copy-on-write"
}

case object MergeOnRead extends RowLevelOperationMode {
  val description: String = "merge-on-read"
}

///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////

class DeltaRowLevelOperationBuilder(
    kernelTable: KernelTable,
    kernelEngine: KernelEngine,
    info: RowLevelOperationInfo)
    extends RowLevelOperationBuilder {

  val mode: RowLevelOperationMode = MergeOnRead

  override def build(): RowLevelOperation = mode match {
    case CopyOnWrite => new CopyOnWriteRowLevelOperation(kernelTable, kernelEngine, info)
    case MergeOnRead => new MergeOnReadRowLevelOperation(kernelTable, kernelEngine, info)
  }

}

///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////

class CopyOnWriteRowLevelOperation(
    kernelTable: KernelTable,
    kernelEngine: KernelEngine,
    info: RowLevelOperationInfo)
    extends RowLevelOperation {
  import DeltaCopyOnWriteOperation._

  logger.info("Scott > CopyOnWriteRowLevelOperation constructed")

  private var readScan: DeltaScan = null
  private var lazyScanBuilder: DeltaScanBuilder = null
  private var lazyWriteBuilder: SparkWriteBuilder = null

  override def command(): RowLevelOperation.Command = info.command()

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    logger.info("Scott > DeltaDeleteRowLevelOperation > newScanBuilder")

    if (lazyScanBuilder == null) {
        lazyScanBuilder = new DeltaScanBuilder(kernelTable, kernelEngine) {
          override def build(): SparkScan = {
            val scan = super.build().asInstanceOf[DeltaScan]
            CopyOnWriteRowLevelOperation.this.readScan = scan
            scan
          }
        }
    }

    lazyScanBuilder
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    logger.info("Scott > DeltaDeleteRowLevelOperation > newWriteBuilder")

    if (lazyWriteBuilder == null) {
      lazyWriteBuilder = new SparkWriteBuilder(kernelTable, info)
      logger.info(s"Scott > DeltaDeleteRowLevelOperation > newWriteBuilder :: readScan=$readScan")
      lazyWriteBuilder.injectReadScan(readScan)
    }

    lazyWriteBuilder
  }
}

object DeltaCopyOnWriteOperation {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
}

///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////

class MergeOnReadRowLevelOperation(
    kernelTable: KernelTable,
    kernelEngine: KernelEngine,
    info: RowLevelOperationInfo)
  extends RowLevelOperation with SupportsDelta {

  import MergeOnReadRowLevelOperation._

  logger.info("Scott > MergeOnReadRowLevelOperation constructed")

  private var readScan: DeltaScan = null
  private var lazyScanBuilder: DeltaScanBuilder = null
  private var lazyWriteBuilder: DeltaOfRowsWriteBuilder = null

  override def command(): RowLevelOperation.Command = info.command()

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    logger.info("Scott > MergeOnReadRowLevelOperation > newScanBuilder")

    if (lazyScanBuilder == null) {
      lazyScanBuilder = new DeltaScanBuilder(kernelTable, kernelEngine) {
        override def build(): SparkScan = {
          val scan = super.build().asInstanceOf[DeltaScan]
          MergeOnReadRowLevelOperation.this.readScan = scan
          scan
        }
      }
    }

    lazyScanBuilder
  }

  override def newWriteBuilder(info: LogicalWriteInfo): DeltaWriteBuilder = {
    logger.info("Scott > MergeOnReadRowLevelOperation > newWriteBuilder")

    if (lazyWriteBuilder == null) {
      lazyWriteBuilder = new DeltaOfRowsWriteBuilder(kernelTable)
    }

    lazyWriteBuilder
  }

  override def rowId(): Array[NamedReference] = {
    val filePath = Expressions.column("_metadata.file_path") // File path of the Parquet file
    val rowIndex = Expressions.column("_metadata.row_index") // Row index within the file
    Array(filePath, rowIndex)
  }
}

object MergeOnReadRowLevelOperation {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
}
