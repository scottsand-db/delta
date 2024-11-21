package io.delta

import io.delta.sql.DeltaSparkSessionExtension
import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SharedSparkSession

import java.util.UUID

class DMLSuite extends QueryTest with SharedSparkSession {
  import DMLSuite._

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(
        StaticSQLConf.SPARK_SESSION_EXTENSIONS.key,
        classOf[DeltaSparkSessionExtension].getName)
      .set(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[DeltaCatalog].getName)
      .set("spark.sql.catalog.my_delta_catalog", "io.delta.DeltaCatalog")
      // <table_path>/test%dv%prefix-deletion_vector_e47cb0fa-7f45-47d9-bbb9-ab1f15db41d1.bin
      //
      // {"add":{"path":"test%25file%25prefix-part-00001-ac7797e7-efdc-4a36-99a0-7d0b262def4d-c000.
      // snappy.parquet","partitionValues":{},"size":818,"modificationTime":1732217739309,"dataChang
      // e":true,"stats":"{\"numRecords\":5,\"minValues\":{\"id\":5,\"value\":\"test_value\"},\"maxV
      // alues\":{\"id\":9,\"value\":\"test_value\"},\"nullCount\":{\"id\":0,\"value\":0},\"tightBou
      // nds\":false}","deletionVector":{"storageType":"u","pathOrInlineDv":"<B1idE)((XYsB%:72844","
      // offset":1,"sizeInBytes":36,"cardinality":2}}}
      //
      // Caused by: java.io.FileNotFoundException: File file:/tmp/spark_warehouse/table_6696d2fc/de
      // letion_vector_e47cb0fa-7f45-47d9-bbb9-ab1f15db41d1.bin does not exist
      .set("spark.databricks.delta.testOnly.dataFileNamePrefix", "")
      .set("spark.databricks.delta.testOnly.dvFileNamePrefix", "")
  }

  def withUniqueTableIdAndItsPath(test: (String, String) => Unit): Unit = {
    val tableName = s"table_${UUID.randomUUID().toString.substring(0, 8)}"
    val tid = s"my_delta_catalog.$tableName"
    val path = s"/tmp/spark_warehouse/$tableName"
    println(s"Using table id: $tid")
    println(s"Using table path: $path")
    test(tid, path)
  }

  test("aaa") {
    withUniqueTableIdAndItsPath { (tableId, path) =>
      logger.info("Scott > STEP 1a: create table with dsv1")

      spark
        .range(10)
        .withColumn("value", lit("test_value"))
        .write
        .format("delta")
        .mode("overwrite")
        .option("delta.enableDeletionVectors", "true")
        .save(path)

      logger.info("Scott > STEP 1b: read table with dsv1")

      val initialDataDSv1 = spark.read.format("delta").load(path)
      assert(initialDataDSv1.count() == 10)
      initialDataDSv1.show()

      logger.info("Scott > STEP 1c: read table with dsv2")
      val initialDataDSv2 = spark.read.format("delta2").table(tableId)
      assert(initialDataDSv2.count() == 10)
      initialDataDSv2.show()

      // Step 2: Perform a MERGE operation using DeltaTable API (DSV1)
      logger.info("Scott > STEP 2a: prepare source data")
      import io.delta.tables._
      val sourceData = spark.range(8, 12).withColumn("value", concat(lit("updated_"), col("id")))

      logger.info("Scott > STEP 2b: perform merge with DeltaTable API (dsv1)")
      val deltaTable = DeltaTable.forPath(spark, path)
      deltaTable.as("target")
        .merge(
          sourceData.as("source"),
          "target.id = source.id"
        )
        .whenMatched()
        .updateExpr(Map("value" -> "source.value"))
        .whenNotMatched()
        .insertExpr(Map("id" -> "source.id", "value" -> "source.value"))
        .execute()

      // Validate the data after MERGE
      logger.info("Scott > STEP 3a: read table after merge with dsv1")
      val postMergeDSv1 = spark.read.format("delta").load(path)
      postMergeDSv1.show()
      assert(postMergeDSv1.count() == 12)

      logger.info("Scott > STEP 3b: read table after merge with dsv2")
      val readDataDSv2 = spark.read.format("delta2").table(tableId)
      readDataDSv2.show()
      assert(readDataDSv2.count() == 12)
    }
  }

  test("bbb") {
    withUniqueTableIdAndItsPath { (tid, path) =>
      spark
        .range(10)
        .withColumn("part1", col("id") % 5)
        .withColumn("col1", col("id").cast("long"))
        .withColumn("col2", concat(lit("value_"), col("id").cast("string")))
        .withColumn("col3", col("id") % 2 === 0)
        .drop("id")
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy("part1")
        .option("delta.enableDeletionVectors", "true")
        .save(path)

      logger.info(s"Scott > Table $tid created")

      spark.table(tid).show(100)

      logger.info("Scott > performing delete")

      spark.sql(s"DELETE FROM $tid WHERE col1 < 3")

      spark.table(tid).show(100)
    }
  }
}

object DMLSuite {
  val logger = org.slf4j.LoggerFactory.getLogger("DMLSuite")
}
