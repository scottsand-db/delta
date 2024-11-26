package io.delta.engine

import io.delta.kernel.engine.{CommitCoordinatorClientHandler, Engine, ExpressionHandler, FileSystemClient, JsonHandler, ParquetHandler}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession

import java.util

// scalastyle:off deltahadoopconfiguration
object KernelSparkEngine {
  def createOnDriver(): Engine =
    io.delta.kernel.defaults.engine.DefaultEngine.create(new Configuration())
//    new KernelSparkEngine(SparkSession.active.sessionState.newHadoopConf())

  def createOnExecutor(): Engine =
    io.delta.kernel.defaults.engine.DefaultEngine.create(new Configuration())
//    new KernelSparkEngine(new Configuration())
}

class KernelSparkEngine(hadoopConf: Configuration) extends Engine {

  private val impl = io.delta.kernel.defaults.engine.DefaultEngine.create(hadoopConf)

  private lazy val kernelSparkParquetHandler =
    new KernelSparkParquetHandler(hadoopConf, impl.getParquetHandler)

  override def getExpressionHandler: ExpressionHandler = impl.getExpressionHandler

  override def getJsonHandler: JsonHandler = impl.getJsonHandler

  override def getFileSystemClient: FileSystemClient = impl.getFileSystemClient

  override def getParquetHandler: ParquetHandler = kernelSparkParquetHandler

  override def getCommitCoordinatorClientHandler(
      name: String,
      conf: util.Map[String, String]): CommitCoordinatorClientHandler =
    impl.getCommitCoordinatorClientHandler(name, conf)
}
