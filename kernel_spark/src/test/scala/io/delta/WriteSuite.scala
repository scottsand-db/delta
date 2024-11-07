package io.delta

import io.delta.sql.DeltaSparkSessionExtension
import org.apache.spark.SparkConf
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}

import java.util.UUID
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession

class WriteSuite extends QueryTest with SharedSparkSession {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key,
        classOf[DeltaSparkSessionExtension].getName)
      .set(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key,
        classOf[DeltaCatalog].getName)
  }

  def withUniquePath(test: String => Unit): Unit = {
    val path = s"/tmp/delta_tables/table_${UUID.randomUUID().toString.substring(0, 8)}"
    println(s"Using path: $path")
    test(path)
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

      spark.range(10).write.mode("append").format("delta2").save(path)

      // Read it back using delta-spark (dsv1)

      spark.read.format("delta").load(path).show()
    }
  }
}
