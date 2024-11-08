package io.delta

import io.delta.sql.DeltaSparkSessionExtension
import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SharedSparkSession

import java.util.UUID

class ReadSuite extends QueryTest with SharedSparkSession {
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

  test("aaa") {
    withUniquePath { path =>
      // write using delta-spark (DSV1)
      spark.range(10).write.format("delta").save(path)

      // read using DSV2
      spark.read.format("delta2").load(path).show()
    }
  }
}