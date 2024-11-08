package io.delta

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class DeltaSource extends DataSourceRegister with TableProvider {
  import io.delta.DeltaSource._

  override def shortName(): String = "delta2"

  override def inferSchema(caseInsensitiveStringMap: CaseInsensitiveStringMap): StructType = {
    logger.info(s"inferSchema: caseInsensitiveStringMap=${caseInsensitiveStringMap.entrySet()}")
    null
  }

  override def inferPartitioning(options: CaseInsensitiveStringMap): Array[Transform] = {
    logger.info(s"inferPartitioning: options=${options.entrySet()}")
    val table = getTable(null, null, options)
    table.partitioning()
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]): Table = {
    logger.info(s"getTable: schema=$schema, partitioning=$partitioning, properties=$properties")

    val path = properties.get("path")

    if (path == null) throw new IllegalArgumentException("path is null")

    new DeltaTable(path)
  }
}

object DeltaSource {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
}
