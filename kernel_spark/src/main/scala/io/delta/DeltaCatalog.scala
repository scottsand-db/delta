package io.delta

import io.delta.kernel.Operation
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.{IdentityTransform, NamedReference, Transform}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import java.util.UUID

import scala.collection.JavaConverters._

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
    new DeltaTable(ident.name())
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {
    val path = if (properties.containsKey("path")) {
      properties.get("path")
    } else {
      s"/tmp/table_${UUID.randomUUID().toString.replace("-", "").substring(0, 4)}"
    }

    logger.info(
      s"createTable: ident=$ident, schema=$schema, " +
        s"partitions=${partitions.mkString("Array(", ", ", ")")}, properties=$properties, " +
        s"path=$path")

    val partitionCols = partitions.map(extractPartitionColumn)

    val result = io.delta.kernel.Table
      .forPath(engine, path)
      .createTransactionBuilder(engine, "kernel-spark-dsv2", Operation.CREATE_TABLE)
      .withSchema(engine, SchemaUtils.convertSparkSchemaToKernelSchema(schema))
      .withPartitionColumns(engine, partitionCols.toList.asJava)
      .build(engine)
      .commit(engine, io.delta.kernel.utils.CloseableIterable.emptyIterable())

    logger.info(s"createTable: resultVersion=${result.getVersion}")

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

  private def extractPartitionColumn(transform: Transform): String = {
    logger.info(s"transform: $transform")
    // Check if the transform is an identity transform
    if (transform.name() == "identity" && transform.references().nonEmpty) {
      // Get the first reference, which should be the column
      transform.references()(0) match {
        case namedRef: NamedReference =>
          logger.info(s"namedRef: $namedRef")
          logger.info(
            s"namedRef.fieldNames: ${namedRef.fieldNames().mkString("Array(", ", ", ")")}")
          namedRef.fieldNames().mkString(".")
        case _ => throw new RuntimeException("bad aa")
      }
    } else {
      throw new RuntimeException("bad bb")
    }
  }
}

object DeltaCatalog {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  // identifier -> table
  val inMemoryTables = scala.collection.mutable.Map[String, DeltaTable]()
}
