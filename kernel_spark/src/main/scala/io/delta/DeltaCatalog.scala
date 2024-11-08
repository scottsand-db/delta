package io.delta

import io.delta.kernel.Operation
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import java.util.UUID

class DeltaCatalog extends TableCatalog {
  import DeltaCatalog._

  private var catalogName: String = _
  private lazy val engine =
    io.delta.kernel.defaults.engine.DefaultEngine.create(new Configuration())

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    this.catalogName = name
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    Array.empty
  }

  override def loadTable(ident: Identifier): Table = {
    val path = ident.name()
    new DeltaTable(path) // Uses DeltaTable class
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {
    val path = if (properties.containsKey("path")) {
      properties.get("path")
    } else {
      s"/tmp/table_${UUID.randomUUID().toString.replace("-", "")}"
    }

    logger.info(
      s"createTable: ident=$ident, schema=$schema, partitions=$partitions, " +
        s"properties=$properties, path=$path")

    val result = io.delta.kernel.Table
      .forPath(engine, path)
      .createTransactionBuilder(engine, "kernel-spark-dsv2", Operation.CREATE_TABLE)
      .withSchema(engine, SchemaUtils.convertSparkSchemaToKernelSchema(schema))
      .build(engine)
      .commit(engine, io.delta.kernel.utils.CloseableIterable.emptyIterable())

    logger.info(s"createTable: resultVersion=${result.getVersion()}")

    val table = new DeltaTable(path)
    inMemoryTables.put(ident.name(), table)
    table
  }

  override def dropTable(ident: Identifier): Boolean = {
    if (inMemoryTables.contains(ident.name())) {
      inMemoryTables.remove(ident.name())
      true
    } else {
      false
    }
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    throw new UnsupportedOperationException("renameTable is not supported")
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    throw new UnsupportedOperationException("alterTable is not supported")
  }

  override def tableExists(ident: Identifier): Boolean = {
    inMemoryTables.contains(ident.name())
  }

  override def name(): String = catalogName
}

object DeltaCatalog {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

// identifier -> table
  val inMemoryTables = scala.collection.mutable.Map[String, DeltaTable]()
}
