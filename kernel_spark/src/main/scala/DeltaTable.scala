import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType

import collection.JavaConverters._

class DeltaTable(path: String) extends Table with SupportsWrite {
  private lazy val engine =
    io.delta.kernel.defaults.engine.DefaultEngine.create(new Configuration())

  private lazy val table =
    io.delta.kernel.Table.forPath(engine, path)

  private lazy val spark = SparkSession.active()

  override def name(): String = s"delta.`$path`"

  override def schema(): StructType = {
    SchemaUtils.convertKernelSchemaToSparkSchema(
      table.getLatestSnapshot(engine).getSchema(engine))
  }

  override def capabilities(): java.util.Set[TableCapability] = {
    Set(TableCapability.BATCH_WRITE).asJava
  }

  override def newWriteBuilder(writeInfo: LogicalWriteInfo): WriteBuilder = {
    new DeltaWriteBuilder(table, writeInfo)
  }
}
