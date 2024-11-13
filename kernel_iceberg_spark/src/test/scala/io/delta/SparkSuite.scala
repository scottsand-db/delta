package io.delta


import org.apache.iceberg.catalog.TableIdentifier
import org.apache.spark.SparkConf

import java.util.UUID
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession

object SparkSuite {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
}

// scalastyle:off deltahadoopconfiguration
// scalastyle:off line.size.limit
// build/sbt -Djava.version=17 -Dio.netty.tryReflectionSetAccessible=true 'kernelIcebergSpark/testOnly *SparkSuite -- -z "ccc"'
class SparkSuite extends QueryTest with SharedSparkSession {
  import SparkSuite._

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") // Set Spark’s default to Iceberg
      .set("spark.sql.catalog.spark_catalog.type", "hadoop") // Example: specify catalog type for Iceberg
      .set("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") // Use Iceberg’s SparkCatalog for my_catalog
      .set("spark.sql.catalog.my_catalog.catalog-impl", "io.delta.DeltaCatalog") // Implement with DeltaCatalog
      .set("spark.sql.catalog.my_catalog.warehouse", "/tmp/spark_warehouse") // Define warehouse path for Iceberg
      .set("spark.sql.defaultCatalog", "my_catalog") // Set my_catalog as the default
      .set("spark.driver.extraJavaOptions", "-XX:+IgnoreUnrecognizedVMOptions")
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.databricks.delta.testOnly.dataFileNamePrefix", "test_file_prefix_") // The %25 was causing encoding errors?
      .set("spark.sql.parquet.enableVectorizedReader", "false")
  }

  def withTableNameAndLocation(test: (String, String) => Unit): Unit = {
    val tableName = s"table_${UUID.randomUUID().toString.substring(0, 6)}"
    val tableLocation = s"/tmp/spark_warehouse/$tableName"

    logger.info(s"Using table name: $tableName")
    logger.info(s"Using table location: $tableLocation")

    test(tableName, tableLocation)
  }

  // SIMPLE WRITE THEN READ
  test("aaa") {
    withTableNameAndLocation { (tableName, tableLocation) =>
      val tableIdentifier = s"my_catalog.$tableName"
      spark.sql(
        s"CREATE TABLE $tableIdentifier (id BIGINT) USING iceberg LOCATION '$tableLocation'");

      logger.info(s"Scott >> Created ICEBERG CATALOG TABLE with tableIdentifier $tableIdentifier")

      val deltaTableAsIcebergTable = new io.delta.DeltaTable(
        TableIdentifier.of(tableIdentifier), spark.sessionState.newHadoopConf(), tableLocation);

      deltaTableAsIcebergTable.schema().columns().forEach { col =>
        logger.info(s"Scott > Name: ${col.name()}, Type: ${col.`type`()}, fieldId: ${col.fieldId()}")
      }

      spark.range(10)
        .write
        .format("iceberg")
        .option("path", tableLocation)
        .mode("append")
        .saveAsTable(tableIdentifier)

      logger.info("SHOWING ICEBERG TABLE READ")

      spark.read.format("iceberg").table(tableIdentifier).show()

      logger.info("SHOWING DELTA TABLE READ")

      spark.read.format("delta").load(tableLocation).show()
    }
  }

  // PARTITIONED WRITE THEN READ
  test("aaa2") {
    withTableNameAndLocation { (tableName, tableLocation) =>
      val tableIdentifier = s"my_catalog.$tableName"
      spark.sql(s"CREATE TABLE $tableIdentifier (part1 BIGINT, col1 BIGINT, col2 BIGINT, col3 BIGINT) " +
        s"USING iceberg " +
        s"LOCATION '$tableLocation' " +
        s"PARTITIONED BY (part1)")

      logger.info(s"Scott >> Created ICEBERG CATALOG TABLE with tableIdentifier $tableIdentifier")

      val deltaTableAsIcebergTable = new io.delta.DeltaTable(
        TableIdentifier.of(tableIdentifier), spark.sessionState.newHadoopConf(), tableLocation);

      deltaTableAsIcebergTable.schema().columns().forEach { col =>
        logger.info(s"Scott > Name: ${col.name()}, Type: ${col.`type`()}, fieldId: ${col.fieldId()}")
      }

      spark
        .range(10)
        .withColumn("part1", col("id") % 5)
        .withColumn("col1", col("id"))
        .withColumn("col2", col("id") * 10)
        .withColumn("col3", col("id") * 100)
        .drop("id")
        .write
        .format("iceberg")
        .option("path", tableLocation)
        .mode("append")
        .saveAsTable(tableIdentifier)

      logger.info("SHOWING DELTA TABLE READ")

      spark.read.format("delta").load(tableLocation).show()
    }
  }

  // SIMPLE CASE: write via delta-spark dsv1 and read via iceberg + DeltaCatalog dsv2
  test("bbb") {
    withTableNameAndLocation { (tableName, tableLocation) =>
      spark.range(10).write.format("delta").save(tableLocation)

      logger.info(s"Created table at $tableLocation")

      spark.read.format("iceberg").table(s"my_catalog.$tableName").show()
    }
  }

  // PARTITIONED CASE: write via delta-spark dsv1 and read via iceberg + DeltaCatalog dsv2
  test("ccc") {
    withTableNameAndLocation { (tableName, tableLocation) =>
      val tableIdentifier = s"my_catalog.$tableName"
      // scalastyle:off line.size.limit
      spark.sql(s"CREATE TABLE $tableIdentifier (part1 BIGINT, col1 BIGINT, col2 BIGINT, col3 BIGINT) " +
        s"USING iceberg " +
        s"LOCATION '$tableLocation' " +
        s"PARTITIONED BY (part1)")
      // scalastyle:on line.size.limit

      logger.info(s"Scott >> Created ICEBERG CATALOG TABLE with tableIdentifier $tableIdentifier")

      spark
        .range(10)
        .withColumn("part1", col("id") % 5)
        .withColumn("col1", col("id"))
        .withColumn("col2", col("id") * 10)
        .withColumn("col3", col("id") * 100)
        .drop("id")
        .write
        .format("delta")
        .mode("append")
        .partitionBy("part1")
        .save(tableLocation)

      logger.info(s"Scott >> APPENDED TO DELTA table at $tableLocation")

//      spark.read.format("iceberg").table(tableIdentifier).explain(true)

      spark.read.format("iceberg").table(tableIdentifier).show()
    }
  }
}