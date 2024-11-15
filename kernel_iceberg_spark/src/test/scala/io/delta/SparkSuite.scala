package io.delta


import org.apache.iceberg.{PartitionSpec, Schema}
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.spark.SparkConf

import java.util.UUID
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession

import scala.collection.JavaConverters._

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
      .set("spark.driver.extraJavaOptions", "-XX:+IgnoreUnrecognizedVMOptions -Dlog4j.logger.org.apache.spark=DEBUG")
      .set("spark.executor.extraJavaOptions", "-Dlog4j.logger.org.apache.spark=DEBUG")
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.databricks.delta.testOnly.dataFileNamePrefix", "test_file_prefix_") // The %25 was causing encoding errors?
      .set("spark.sql.parquet.enableVectorizedReader", "false")
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir", "/tmp/spark_event_log")
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

      spark.read.format("iceberg").table(tableIdentifier).where("id > 5").show()
    }
  }

  // PARTITIONED WRITE THEN READ
  test("bbb") {
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
        .range(50)
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

      logger.info("SHOWING ICEBERG TABLE READ")

      spark.read.format("iceberg").table(tableIdentifier).where("part1 > 2").show()
    }
  }

  test("ccc") {
    import scala.Predef._

    withTableNameAndLocation { (tableName, tableLocation) =>
      val tableIdentifier = s"my_catalog.$tableName"
      spark.sql(s"CREATE TABLE $tableIdentifier (part1 BIGINT, col1 BIGINT, col2 BIGINT, col3 BIGINT) " +
        s"USING iceberg " +
        s"LOCATION '$tableLocation' " +
        s"PARTITIONED BY (part1)")

      logger.info(s"Scott > Created ICEBERG CATALOG TABLE with tableIdentifier $tableIdentifier")

      (1 to 7).foreach { x =>
        logger.info(s"SCOTT > STARTING COMMIT #$x")

        spark
          .range(x.toLong * 50)
          .withColumn("part1", col("id") % 5)
          .withColumn("col1", col("id"))
          .withColumn("col2", col("id") * 10)
          .withColumn("col3", col("id") * 100)
          .drop("id")
          .write
          .format("iceberg")
          .partitionBy("part1")
          .option("path", tableLocation)
          .mode("append")
          .saveAsTable(tableIdentifier)

        logger.info(s"SCOTT > FINISHED COMMIT #$x")
      }

      logger.info("SHOWING ICEBERG TABLE READ")

      spark.read.format("iceberg").table(tableIdentifier).show()

      log.info("Scott > DELETING FROM THE TABLE ....")

      spark.sql(s"DELETE FROM $tableIdentifier WHERE part1 = 2")

      spark.read.format("iceberg").table(tableIdentifier).show(numRows = 100, truncate = false)

      val deltaTableAsIcebergTable = new io.delta.DeltaTable(
        TableIdentifier.of(tableIdentifier), spark.sessionState.newHadoopConf(), tableLocation);

      deltaTableAsIcebergTable.snapshots().forEach { snapshot =>
        logger.info(s"Snapshot ID: ${snapshot.snapshotId()}")
        logger.info(s"Timestamp: ${snapshot.timestampMillis()}")
        logger.info(s"Operation: ${snapshot.operation()}")
        logger.info(s"Summary: ${snapshot.summary()}")
      }
    }
  }

  test("ddd") {
    withTableNameAndLocation { (tableName, tableLocation) =>
      val tableIdentifier = s"my_catalog.$tableName"
      spark.sql(s"CREATE TABLE $tableIdentifier (part1 BIGINT, col1 BIGINT, col2 BIGINT, col3 BIGINT) " +
        s"USING iceberg " +
        s"LOCATION '$tableLocation' " +
        s"PARTITIONED BY (part1)")

      logger.info(s"Scott >> Created ICEBERG CATALOG TABLE with tableIdentifier $tableIdentifier")

      spark
        .range(50)
        .withColumn("part1", col("id") % 5)
        .withColumn("col1", col("id"))
        .withColumn("col2", col("id") * 10)
        .withColumn("col3", col("id") * 100)
        .drop("id")
        .write
        .format("iceberg")
        .partitionBy("part1")
        .option("path", tableLocation)
        .mode("append")
        .saveAsTable(tableIdentifier)

      logger.info("STARTING ICEBERG TABLE UPDATE")

      val deltaTableAsIcebergTable = new io.delta.DeltaTable(
        TableIdentifier.of(tableIdentifier), spark.sessionState.newHadoopConf(), tableLocation);

      logger.info(s"Scott > SCHEMA ${deltaTableAsIcebergTable.schema()}")
      logger.info(s"Scott > SPEC ${deltaTableAsIcebergTable.spec()}")

      def checkCompatibility(spec: PartitionSpec, schema: Schema): Unit = {
        for (field <- spec.fields().asScala) {
          val sourceType = schema.findType(field.sourceId())
          val transform = field.transform()
          logger.info(s"Scott > field $field, sourceType $sourceType, transform $transform")
        }
      }
      checkCompatibility(deltaTableAsIcebergTable.spec(), deltaTableAsIcebergTable.schema())
      spark.sql(s"UPDATE $tableIdentifier SET col1 = 8 WHERE col1 = 5")
      spark.read.format("iceberg").table(tableIdentifier).show()
    }
  }

  def checkCompatibility(spec: PartitionSpec, schema: Schema): Unit = {
    for (field <- spec.fields().asScala) {
      val sourceType = schema.findType(field.sourceId())
      val transform = field.transform()
      logger.info(s"Scott > field $field, sourceType $sourceType, transform $transform")
    }
  }

  // SIMPLE CASE: write via delta-spark dsv1 and read via iceberg + DeltaCatalog dsv2
  test("zzz") {
    withTableNameAndLocation { (tableName, tableLocation) =>
      spark.range(10).write.format("delta").save(tableLocation)

      logger.info(s"Created table at $tableLocation")

      spark.read.format("iceberg").table(s"my_catalog.$tableName").show()
    }
  }

  // PARTITIONED CASE: write via delta-spark dsv1 and read via iceberg + DeltaCatalog dsv2
  test("yyy") {
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