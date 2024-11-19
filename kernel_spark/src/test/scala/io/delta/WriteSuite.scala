package io.delta

import io.delta.sql.DeltaSparkSessionExtension
import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}

import java.util.UUID
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.functions.{concat, _}
import org.apache.spark.sql.test.SharedSparkSession

object WriteSuite {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
}

class WriteSuite extends QueryTest with SharedSparkSession {
  import WriteSuite._

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(
        StaticSQLConf.SPARK_SESSION_EXTENSIONS.key,
        classOf[DeltaSparkSessionExtension].getName)
      .set(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[DeltaCatalog].getName)
      .set("spark.sql.catalog.my_delta_catalog", "io.delta.DeltaCatalog")
  }

  def withUniquePath(test: String => Unit): Unit = {
    val path = s"/tmp/delta_tables/table_${UUID.randomUUID().toString.substring(0, 8)}"
    println(s"Using path: $path")
    test(path)
  }

  def withUniqueTableId(test: String => Unit): Unit = {
    val tid = s"my_delta_catalog.table_${UUID.randomUUID().toString.substring(0, 8)}"
    println(s"Using table id: $tid")
    test(tid)
  }

  test("aaa") {
    withUniquePath { path =>
      val data = spark.range(10)

      data.show()

      // Create the table using delta-spark (dsv1)

      spark
        .createDataFrame(spark.sparkContext.emptyRDD[Row], data.schema)
        .write
        .mode("overwrite")
        .format("delta")
        .save(path)

      // Write to the table using kernelSpark (dsv2)

      data.write.mode("append").format("delta2").save(path)

      // Read it back using delta-spark (dsv1)

      spark.read.format("delta").load(path).show()
    }
  }

  test("bbb") {
    withUniquePath { path =>
      val data = spark
        .range(10)
        .withColumn("part1", col("id") % 5)
        .withColumn("col1", col("id").cast("long"))
        .withColumn("col2", concat(lit("value_"), col("id").cast("string")))
        .withColumn("col3", col("id") % 2 === 0)
        .drop("id")

      data.show()

      data.printSchema()

      // Create the table using delta-spark (dsv1)

      spark
        .createDataFrame(spark.sparkContext.emptyRDD[Row], data.schema)
        .write
        .mode("overwrite")
        .format("delta")
        .partitionBy("part1")
        .save(path)

      // Write to the table using kernelSpark (dsv2)

      data.write.mode("append").format("delta2").partitionBy("part1").save(path)

      // Read it back using delta-spark (dsv1)

      spark.read.format("delta").load(path).show()
    }
  }

  test("ccc") {
    withUniquePath { path =>
      val data = spark
        .range(100000)
        .withColumn("part1", col("id") % 1000)
        .withColumn("part2", col("id") % 100)
        .withColumn("part3", col("id") % 10)
        .withColumn("col1", col("id").cast("long"))
        .withColumn("col2", concat(lit("value_"), col("id").cast("string")))
        .withColumn("col3", col("id") % 2 === 0)
        .drop("id")

      data.show()

      data.printSchema()

      // Create the table using delta-spark (dsv1)

      spark
        .createDataFrame(spark.sparkContext.emptyRDD[Row], data.schema)
        .write
        .mode("overwrite")
        .format("delta")
        .partitionBy("part1", "part2", "part3")
        .save(path)

      // Write to the table using kernelSpark (dsv2)

      val start1 = System.currentTimeMillis()
      data.write.mode("append").format("delta2").partitionBy("part1", "part2", "part3").save(path)
      val end1 = System.currentTimeMillis()

      // Read it back using delta-spark (dsv1)

      spark.read.format("delta").load(path).show()
      println(s"${spark.read.format("delta").load(path).count()}")

      // Write it using dsv1

      val start2 = System.currentTimeMillis()
      data.write.mode("append").format("delta").partitionBy("part1", "part2", "part3").save(path)
      val end2 = System.currentTimeMillis()

      // Read it back using delta-spark (dsv1)

      spark.read.format("delta").load(path).show()
      println(s"${spark.read.format("delta").load(path).count()}")

      println(s"Time to write using DSV1: ${end2 - start2} ms")
      println(s"Time to write using DSV2: ${end1 - start1} ms")
    }
  }

  test("ddd") {
    val tableName = s"table_${UUID.randomUUID().toString.substring(0, 4)}"
    println(s"using table name $tableName")
    spark.range(10).write.format("delta2").saveAsTable(s"my_delta_catalog.$tableName")
  }

//  test("eee") {
//    withUniquePath { path =>
//      spark.range(10).write.mode("overwrite").format("delta2").save(path)
//    }
//  }

  test("fff") {
    withUniqueTableId { tid =>
      spark
        .range(10)
        .withColumn("part1", col("id") % 5)
        .withColumn("col1", col("id").cast("long"))
        .withColumn("col2", concat(lit("value_"), col("id").cast("string")))
        .withColumn("col3", col("id") % 2 === 0)
        .drop("id")
        .write
        .format("delta2")
        .partitionBy("part1")
        .saveAsTable(tid)

      logger.info(s"Scott > Table $tid created")

      spark.table(tid).show(100)
    }
  }
}
