package io.delta.engine

import io.delta.data.SparkRowsArrayToKernelColumnarBatch
import io.delta.{ExpressionUtils, SchemaUtils}
import io.delta.kernel.data.{FilteredColumnarBatch, ColumnarBatch => KernelColumnarBatch}
import io.delta.kernel.engine.{ParquetHandler => KernelParquetHandler}
import io.delta.kernel.expressions.{Column, Predicate => KernelPredicate}
import io.delta.kernel.types.{StructType => KernelStructType}
import io.delta.kernel.utils.{CloseableIterator, DataFileStatus, FileStatus => KernelFileStatus}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile

import java.util.Optional

object JavaOptionalConverters {
  implicit class JavaOptionalToScalaOption[A](val javaOptional: Optional[A]) extends AnyVal {
    def toScalaOption: Option[A] = {
      if (javaOptional.isPresent) Some(javaOptional.get) else None
    }
  }
}

// scalastyle:off deltahadoopconfiguration
class KernelSparkParquetHandler(
    hadoopConf: Configuration,
    defaultParquetHandler: KernelParquetHandler)
    extends KernelParquetHandler {
  import JavaOptionalConverters._

  override def readParquetFiles(
      fileIter: CloseableIterator[KernelFileStatus],
      physicalSchema: KernelStructType,
      predicate: java.util.Optional[KernelPredicate]): CloseableIterator[KernelColumnarBatch] = {
    val spark = SparkSession.active
    val parquetFileFormat = new ParquetFileFormat()
    val sparkSchema = SchemaUtils.convertKernelSchemaToSparkSchema(physicalSchema)

    // Iterate over files
    fileIter.map { fileStatus =>
      // Create a PartitionedFile object for Spark's reader
      val partitionedFile =
        PartitionedFile(
          InternalRow.empty,
          SparkPath.fromPathString(fileStatus.getPath),
          0, /* start */
          fileStatus.getSize)

      val reader = parquetFileFormat.buildReaderWithPartitionValues(
        sparkSession = spark,
        dataSchema = sparkSchema,
        partitionSchema = new StructType(), // No partition schema
        requiredSchema = sparkSchema, // Read the requested schema
        filters = predicate.toScalaOption
          .map(ExpressionUtils.convertKtoSFilter)
          .toSeq, // Predicate pushdown
        options = Map.empty[String, String], // Additional options
        hadoopConf = hadoopConf)

      val rowsIter = reader.apply(partitionedFile)
      val rowsInMemory = rowsIter.toArray
      new SparkRowsArrayToKernelColumnarBatch(rowsInMemory, physicalSchema)
    }
  }

  override def writeParquetFiles(
      directoryPath: String,
      dataIter: CloseableIterator[FilteredColumnarBatch],
      statsColumns: java.util.List[Column]): CloseableIterator[DataFileStatus] =
    defaultParquetHandler.writeParquetFiles(directoryPath, dataIter, statsColumns)

  override def writeParquetFileAtomically(
      filePath: String,
      data: CloseableIterator[FilteredColumnarBatch]): Unit =
    defaultParquetHandler.writeParquetFileAtomically(filePath, data)
}
