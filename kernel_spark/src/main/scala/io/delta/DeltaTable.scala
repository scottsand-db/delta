package io.delta

import io.delta.kernel.exceptions.TableNotFoundException
import io.delta.kernel.internal.SnapshotImpl
import io.delta.kernel.internal.util.VectorUtils
import io.delta.read.DeltaScanBuilder
import io.delta.write.DeltaWriteBuilder
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.expressions.{Expressions, Transform}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

class DeltaTable(path: String) extends Table with SupportsWrite with SupportsRead {
  import io.delta.DeltaTable._

  private lazy val engine =
    io.delta.kernel.defaults.engine.DefaultEngine.create(new Configuration())

  private lazy val table =
    io.delta.kernel.Table.forPath(engine, path)

  override def name(): String = s"delta.`$path`"

  override def schema(): StructType = {
    try {
      val schema = SchemaUtils.convertKernelSchemaToSparkSchema(
        table.getLatestSnapshot(engine).getSchema(engine))
      logger.info(s"schema: $schema")
      schema
    } catch {
      case e: TableNotFoundException =>
        logger.warn("schema: Table not found", e)
        logger.warn("schema: Returning empty struct")
        new StructType()
    }
  }

  override def capabilities(): java.util.Set[TableCapability] = {
    Set(TableCapability.BATCH_WRITE, TableCapability.BATCH_READ).asJava
  }

  override def newWriteBuilder(writeInfo: LogicalWriteInfo): WriteBuilder = {
    logger.info(s"newWriteBuilder: writeInfo=$writeInfo")
    new DeltaWriteBuilder(table, writeInfo)
  }

  override def partitioning(): Array[Transform] = {
    try {
      val partColNames = VectorUtils.toJavaList[String](
        table
          .getLatestSnapshot(engine)
          .asInstanceOf[SnapshotImpl]
          .getMetadata
          .getPartitionColumns)

      val result =
        partColNames.asScala.map(partColName => Expressions.identity(partColName)).toArray

      logger.info(s"partitioning: partColNames=$partColNames, result=$result")

      result
    } catch {
      case e: TableNotFoundException =>
        logger.warn("schema: Table not found", e)
        logger.warn("schema: Returning empty partitioning")
        Array.empty
    }
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    logger.info(s"newScanBuilder: options=${options.entrySet()}")
    new DeltaScanBuilder(table, engine)
  }
}

object DeltaTable {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
}
