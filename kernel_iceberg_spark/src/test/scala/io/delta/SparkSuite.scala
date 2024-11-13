package io.delta


import org.apache.spark.SparkConf

import java.util.UUID
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

object SparkSuite {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
}

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

  test("bbb") {
    withTableNameAndLocation { (tableName, tableLocation) =>
      spark.range(10).write.format("delta").save(tableLocation)

      logger.info(s"Created table at $tableLocation")

      spark.read.format("iceberg").table(s"my_catalog.$tableName").show()
    }
  }
}