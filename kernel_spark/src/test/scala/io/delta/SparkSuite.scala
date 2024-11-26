package io.delta

import io.delta.sql.DeltaSparkSessionExtension
import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}

import java.util.UUID
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.functions.{concat, _}
import org.apache.spark.sql.test.SharedSparkSession

object SparkSuite {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
}

/**
 * On devbox: export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64
 */
class SparkSuite extends QueryTest with SharedSparkSession {

  import SparkSuite._

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[DeltaCatalog].getName)
      .set("spark.sql.catalog.my_delta_catalog", "io.delta.DeltaCatalog")
  }

  private def withUniqueTableId(test: String => Unit): Unit = {
    val tid = s"my_delta_catalog.table_${UUID.randomUUID().toString.substring(0, 8)}"
    println(s"Using table id: $tid")
    test(tid)
  }

  private def createSimpleTable(tid: String): Unit = {
    spark.range(50).writeTo(tid).using("delta2").create()
  }

  private def createPartitionedTable(tid: String): Unit = {
    spark
      .range(50)
      .withColumn("part1", col("id") % 5)
      .withColumn("col1", col("id").cast("long"))
      .withColumn("col2", concat(lit("value_"), col("id").cast("string")))
      .withColumn("col3", col("id") % 2 === 0)
      .drop("id")
      .writeTo(tid)
      .using("delta2")
      .partitionedBy(col("part1"))
      .create()
  }

  def testHelper(testName: String)(testCode: => Unit): Unit = {
    Seq(true, false).foreach { supportColumnarReads =>
      test(s"$testName - supportColumnarReads=$supportColumnarReads") {
        spark.conf.set("io.delta.kernel.spark.supportColumnarReads", supportColumnarReads.toString)
        try {
          testCode
        } finally {
          // Reset the configuration after the test
          spark.conf.unset("io.delta.kernel.spark.supportColumnarReads")
        }
      }
    }
  }

  testHelper("aaa -- create, write to, and read table by name -- simple") {
    withUniqueTableId { tid =>
      createSimpleTable(tid)

      val writtenData = spark.table(tid)

      writtenData.show()

      assert(writtenData.count() == 50)
    }
  }

  testHelper("bbb -- create, write to, and read table by name -- partitioned") {
    withUniqueTableId { tid =>
      createPartitionedTable(tid)

      val writtenData = spark.table(tid)

      writtenData.show()

      assert(writtenData.count() == 50)
    }
  }

  testHelper("ccc -- filter by data column on partitioned table") {
    withUniqueTableId { tid =>
      createPartitionedTable(tid)

      val filteredData = spark.table(tid).where("col1 >= 30")

      filteredData.show()

      assert(filteredData.count() == 20)
    }
  }

  testHelper("ddd -- filter by partition column on partitioned table") {
    withUniqueTableId { tid =>
      createPartitionedTable(tid)

      val filteredData = spark.table(tid).where("part1 = 0")

      filteredData.show()

      assert(filteredData.count() == 10)
    }
  }

  testHelper("eee -- filter by partition and data columns on partitioned table") {
    withUniqueTableId { tid =>
      createPartitionedTable(tid)

      val filteredData = spark.table(tid).where("part1 = 0 and col1 >= 30")

      filteredData.show()

      assert(filteredData.count() == 4)
    }
  }

  test("fff - DML") {
    withUniqueTableId { tid =>
      createPartitionedTable(tid)
      spark.table(tid).show(100)

      // ===== PERFORM DELETE =====

      spark.sql(s"DELETE FROM $tid WHERE col1 < 5")
      val writtenData1 = spark.table(tid).orderBy("col1")
      writtenData1.show(100)
      assert(writtenData1.count() == 45)

      // ===== PERFORM UPDATE =====

      spark.sql(s"UPDATE $tid SET col1 = 7 where col1 = 6")
      val writtenData2 = spark.table(tid).orderBy("col1")
      writtenData2.show(100)
      assert(writtenData2.where("col1 = 6").count() == 0)
      assert(writtenData2.where("col1 = 7").count() == 2)

      // ===== PERFORM MERGE =====

      val sourceDf = spark
        .range(42, 60)
        .withColumn("part1", col("id") % 5)
        .withColumn("col1", col("id"))
        .withColumn("col2", concat(lit("new_value_"), col("id").cast("string")))
        .withColumn("col3", col("id") % 2 === 0)
        .drop("id")

      sourceDf.createOrReplaceTempView("sourceTable")

      spark.sql(
        s"""
           |MERGE INTO $tid AS target
           |USING sourceTable AS source
           |ON target.part1 = source.part1 AND target.col1 = source.col1
           |WHEN MATCHED THEN
           |  UPDATE SET target.col2 = source.col2, target.col3 = source.col3
           |WHEN NOT MATCHED THEN
           |  INSERT (part1, col1, col2, col3) VALUES (source.part1, source.col1, source.col2, source.col3)
           |""".stripMargin
      )

      val writtenData3 = spark.table(tid).orderBy("col1")
      writtenData2.show(100)
      assert(writtenData3.count() == 55)

    }
  }
}