package io.delta


import org.apache.spark.SparkConf

import java.util.UUID
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession

object SparkSuite {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
}

// scalastyle:off line.size.limit
// build/sbt -Djava.version=17 -Dio.netty.tryReflectionSetAccessible=true 'kernelIcebergSpark/testOnly *SparkSuite -- -z "ccc"'
// scalastyle:on line.size.limit
class SparkSuite extends QueryTest with SharedSparkSession {
  import SparkSuite._

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") // Set Spark’s default to Iceberg
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

  test("aaa") {
    val tableName = s"table_${UUID.randomUUID().toString.substring(0, 4)}"
    spark.range(10).write.format("iceberg").mode("overwrite").saveAsTable(s"my_catalog.$tableName")
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