package io.delta

import io.delta.kernel.{Scan => KernelScan, Table => KernelTable}
import io.delta.kernel.engine.{Engine => KernelEngine}
import io.delta.read.{DeltaScan, DeltaScanBuilder}
import io.delta.write.DeltaWriteBuilder
import org.apache.spark.sql.connector.read.{ScanBuilder, Scan => SparkScan}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, RowLevelOperation, RowLevelOperationBuilder, RowLevelOperationInfo, WriteBuilder}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////

class DeltaRowLevelOperationBuilder(
    kernelTable: KernelTable,
    kernelEngine: KernelEngine,
    info: RowLevelOperationInfo)
    extends RowLevelOperationBuilder {
  override def build(): RowLevelOperation =
    new DeltaRowLevelOperation(kernelTable, kernelEngine, info)
}

///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////

class DeltaRowLevelOperation(
    kernelTable: KernelTable,
    kernelEngine: KernelEngine,
    info: RowLevelOperationInfo)
    extends RowLevelOperation {
  import DeltaRowLevelOperation._

  logger.info("Scott > DeltaDeleteRowLevelOperation constructed")

  private var readScan: DeltaScan = null
  private var lazyScanBuilder: DeltaScanBuilder = null
  private var lazyWriteBuilder: DeltaWriteBuilder = null

  override def command(): RowLevelOperation.Command = info.command()

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    logger.info("Scott > DeltaDeleteRowLevelOperation > newScanBuilder")

    if (lazyScanBuilder == null) {
        lazyScanBuilder = new DeltaScanBuilder(kernelTable, kernelEngine) {
          override def build(): SparkScan = {
            val scan = super.build().asInstanceOf[DeltaScan]
            DeltaRowLevelOperation.this.readScan = scan
            scan
          }
        }
    }

    lazyScanBuilder
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    logger.info("Scott > DeltaDeleteRowLevelOperation > newWriteBuilder")

    if (lazyWriteBuilder == null) {
      lazyWriteBuilder = new DeltaWriteBuilder(kernelTable, info)
      logger.info(s"Scott > DeltaDeleteRowLevelOperation > newWriteBuilder :: readScan=$readScan")
      lazyWriteBuilder.injectReadScan(readScan)
    }

    lazyWriteBuilder
  }
}

object DeltaRowLevelOperation {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
}
