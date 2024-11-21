package io.delta.write

import io.delta.kernel.data.{ColumnVector, FilteredColumnarBatch, Row => KernelRow}
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.defaults.internal.json.JsonUtils
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.InternalScanFileUtils
import io.delta.kernel.internal.actions.AddFile.{FULL_SCHEMA => ADD_FILE_SCHEMA}
import io.delta.kernel.internal.actions.RemoveFile.{FULL_SCHEMA => REMOVE_FILE_SCHEMA}
import io.delta.kernel.internal.actions.SingleAction
import io.delta.kernel.internal.data.{GenericRow, TransactionStateRow}
import io.delta.kernel.types.{BooleanType, IntegerType, LongType, StringType}
import io.delta.kernel.utils.{CloseableIterable, CloseableIterator}
import io.delta.kernel.{Operation, Table => KernelTable, Transaction => KernelTransaction}
import io.delta.read.{DeltaInputPartition, DeltaScan}
import io.delta.{AbstractVectorWrapper, DataUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write._

import java.util
import java.util.UUID
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////

class SparkWriteBuilder(kernelTable: KernelTable, logicalWriteInfo: LogicalWriteInfo)
    extends WriteBuilder {
  import SparkWriteBuilder._

  var readScan: Option[DeltaScan] = None

  def injectReadScan(readScan: DeltaScan): Unit = {
    this.readScan = Some(readScan)
  }

  override def build(): Write = {
    // TODO: validate Spark schema is compatible with the Delta table: logicalWriteInfo.schema()
    logger.info(s"build")
    new SparkWrite(kernelTable, logicalWriteInfo, readScan)
  }

}

object SparkWriteBuilder {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
}

///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////

class SparkWrite(
    kernelTable: KernelTable,
    logicalWriteInfo: LogicalWriteInfo,
    readScan: Option[DeltaScan])
    extends Write {
  import SparkWrite._

  override def toBatch: BatchWrite = {
    logger.info(s"toBatch")
    new SparkBatchWrite(kernelTable, logicalWriteInfo, readScan)
  }
}

object SparkWrite {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
}

///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * The writing procedure is:
 *   - Create a writer factory by createBatchWriterFactory(PhysicalWriteInfo), serialize and send
 *     it to all the partitions of the input data(RDD).
 *   - For each partition, create the data writer, and write the data of the partition with this
 *     writer. If all the data are written successfully, call DataWriter.commit(). If exception
 *     happens during the writing, call DataWriter.abort().
 *   - If all writers are successfully committed, call commit(WriterCommitMessage[]). If some
 *     writers are aborted, or the job failed with an unknown reason, call
 *     abort(WriterCommitMessage[]).
 */
private class SparkBatchWrite(
    kernelTable: KernelTable,
    logicalWriteInfo: LogicalWriteInfo,
    readScan: Option[DeltaScan])
    extends BatchWrite {
  import SparkBatchWrite._

  /////////////////////
  // Private Members //
  /////////////////////

  private var committed = false;

  private val engine =
    io.delta.kernel.defaults.engine.DefaultEngine.create(new Configuration())

  private val txn = kernelTable
    .createTransactionBuilder(engine, "kernel-spark-dsv2", Operation.WRITE)
    .build(engine)

  val txnStateRowSerialized = JsonUtils.rowToJson(txn.getTransactionState(engine))

  /////////////////
  // Public APIs //
  /////////////////

  /** Creates a writer factory which will be serialized and sent to executors. */
  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    logger.info(s"createBatchWriterFactory: physicalWriteInfo=$info")
    new SparkBatchWriterFactory(txnStateRowSerialized)
  }

  /**
   * Commits this writing job with a list of commit messages. The commit messages are collected
   * from successful data writers and are produced by DataWriter.commit().
   */
  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    logger.info(s"commit: numMessages=${messages.length}")

    if (committed) {
      throw new IllegalStateException("commit() is called more than once")
    }
    committed = true

    var removeFileRows = Array.empty[KernelRow]

    // TODO: somehow communicate that an overwrite is happening
    if (readScan.isDefined) {
      removeFileRows = readScan.get.planInputPartitions().map { partition =>
        val scanFileRow = JsonUtils.rowFromJson(
          partition.asInstanceOf[DeltaInputPartition].serializedScanFileRow,
          InternalScanFileUtils.SCAN_FILE_SCHEMA)

        val addRow = InternalScanFileUtils.getAddFileEntry(scanFileRow)

        val now = System.currentTimeMillis()

        val removeFileOrdinalMap = new util.HashMap[java.lang.Integer, java.lang.Object]() {
          {
            put(
              REMOVE_FILE_SCHEMA.indexOf("path"),
              addRow.getString(ADD_FILE_SCHEMA.indexOf("path")))

            put(REMOVE_FILE_SCHEMA.indexOf("deletionTimestamp"), java.lang.Long.valueOf(now))

            put(REMOVE_FILE_SCHEMA.indexOf("dataChange"), java.lang.Boolean.TRUE)

            put(REMOVE_FILE_SCHEMA.indexOf("extendedFileMetadata"), java.lang.Boolean.TRUE)

            put(
              REMOVE_FILE_SCHEMA.indexOf("partitionValues"),
              addRow.getMap(ADD_FILE_SCHEMA.indexOf("partitionValues")))

            put(
              REMOVE_FILE_SCHEMA.indexOf("size"),
              java.lang.Long.valueOf(addRow.getLong(ADD_FILE_SCHEMA.indexOf("size"))))
          }
        }

        val removeRow = new GenericRow(REMOVE_FILE_SCHEMA, removeFileOrdinalMap)

        logger.info(
          s"Scott > DeltaWrite > commit :: " +
            s"\n\taddRow ${JsonUtils.rowToJson(addRow)}" +
            s"\n\tremoveRow ${JsonUtils.rowToJson(removeRow)}")

        SingleAction.createRemoveFileSingleAction(removeRow)
      }
    }

    val writtenDataActionsArray = messages
      .map { msg =>
        if (!msg.isInstanceOf[SparkWriterCommitMessage]) {
          throw new IllegalArgumentException("messages must be of type DeltaWriterCommitMessage")
        }
        msg.asInstanceOf[SparkWriterCommitMessage]
      }
      .flatMap(_.serializedActions)
      .map(JsonUtils.rowFromJson(_, SingleAction.FULL_SCHEMA))

    txn.commit(engine, arrayToCloseableIterable(writtenDataActionsArray ++ removeFileRows))
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    logger.info(s"abort: numMessages=${messages.length}")
  }

  private def arrayToCloseableIterable[T](array: Array[T]): CloseableIterable[T] =
    new CloseableIterable[T] {

      override def iterator: CloseableIterator[T] = new CloseableIterator[T] {
        private val arrayIterator = array.iterator

        override def hasNext: Boolean = arrayIterator.hasNext

        override def next(): T = arrayIterator.next()

        override def close(): Unit = {}
      }

      override def close(): Unit = {}
    }
}

private object SparkBatchWrite {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
}

///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Note that, the writer factory will be serialized and sent to executors, then the data writer
 * will be created on executors and do the actual writing. So this interface must be serializable
 * and DataWriter doesn't need to be.
 */
private class SparkBatchWriterFactory(txnStateRowSerialized: String)
    extends DataWriterFactory
    with Serializable {
  import SparkBatchWriterFactory._

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    logger.info(s"createWriter: partitionId=$partitionId, taskId=$taskId")
    new SparkBatchDataWriter(txnStateRowSerialized, partitionId, taskId)
  }
}

object SparkBatchWriterFactory {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
}

///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////

/** Created on the executor. */
// TODO: be able to serialize the delta schema (StructType) to send to the executors?
private class SparkBatchDataWriter(txnStateRowSerialized: String, partitionId: Int, taskId: Long)
    extends DataWriter[InternalRow] {
  import SparkBatchDataWriter._

  /////////////////////
  // Private Members //
  /////////////////////

  private val writerId = UUID.randomUUID().toString.substring(0, 5)
  private val engine =
    io.delta.kernel.defaults.engine.DefaultEngine.create(new Configuration())
  private val txnStateRow =
    JsonUtils.rowFromJson(txnStateRowSerialized, TransactionStateRow.SCHEMA)
  private val partitionColNames = TransactionStateRow.getPartitionColumnsList(txnStateRow)
  private val targetTableSchemaButNotTheWriteSchema =
    TransactionStateRow.getLogicalSchema(engine, txnStateRow)
  private val partitionedWriteRecordBuffer = scala.collection.mutable
    .Map[Map[String, Literal], scala.collection.mutable.ArrayBuffer[InternalRow]]()

  logger.info(
    s"DeltaBatchDataWriter created: partitionId=$partitionId, taskId=$taskId," +
      s"writerId=$writerId, schema=$targetTableSchemaButNotTheWriteSchema, " +
      s"partitionColNames=$partitionColNames,txnStateRowSerialized=$txnStateRowSerialized")

  /////////////////
  // Public APIs //
  /////////////////

  override def write(record: InternalRow): Unit = {
    val recordToWrite = record.copy()

    val partitionValues = DataUtils.sparkRowToKernelPartitionValues(
      record,
      targetTableSchemaButNotTheWriteSchema,
      partitionColNames.asScala)

    partitionedWriteRecordBuffer
      .getOrElseUpdate(partitionValues, scala.collection.mutable.ArrayBuffer[InternalRow]())
      .append(recordToWrite)

    logger.info(
      s"write[$writerId]: record=$recordToWrite, partitionValues=$partitionValues, " +
        s"partitionedWriteRecordBuffer(partitionValues).size=" +
        s"${partitionedWriteRecordBuffer(partitionValues).size}, " +
        s"partitionedWriteRecordBuffer(partitionValues)=" +
        s"${partitionedWriteRecordBuffer(partitionValues)}")
  }

  override def commit(): WriterCommitMessage = {
    logger.info(s"commit[$writerId]")

    val serializedDataActionsArray = partitionedWriteRecordBuffer.flatMap {
      case (partitionValues, arrayBufferData) =>
        commitSinglePartition(partitionValues.asJava, arrayBufferData.toArray)
    }.toArray

    partitionedWriteRecordBuffer.clear()

    new SparkWriterCommitMessage(serializedDataActionsArray)
  }

  private def commitSinglePartition(
      partitionValues: java.util.Map[String, Literal],
      partitionArrayData: Array[InternalRow]): Array[String] = {
    logger.info(s"commitSinglePartition[$writerId]: partitionValues=$partitionValues")

    val logicalData =
      sparkRecordBufferToKernelColumnarBatchIter(partitionArrayData)

    logger.info(s"commitSinglePartition[$writerId] > logicalData: $logicalData")

    val physicalData =
      KernelTransaction.transformLogicalData(engine, txnStateRow, logicalData, partitionValues)

    logger.info(s"commitSinglePartition[$writerId] > physicalData: $physicalData")

    val txnWriteContext =
      KernelTransaction.getWriteContext(engine, txnStateRow, partitionValues)

    logger.info(
      s"commitSinglePartition[$writerId] > targetDirectory=${txnWriteContext.getTargetDirectory()}")

    val dataFiles = engine
      .getParquetHandler()
      .writeParquetFiles(
        txnWriteContext.getTargetDirectory(),
        physicalData,
        txnWriteContext.getStatisticsColumns())

    val writtenDataActionsIter =
      KernelTransaction.generateAppendActions(engine, txnStateRow, dataFiles, txnWriteContext)

    val serializedDataActionsIter = writtenDataActionsIter.map(JsonUtils.rowToJson(_))

    val serializedDataActionsArray = closeableIteratorToArray(serializedDataActionsIter)

    logger.info(
      s"commit[$writerId] > serializedDataActionsArray:" +
        s"${serializedDataActionsArray.mkString("\n- ", "\n- ", "")}")

    serializedDataActionsArray
  }

  override def abort(): Unit = {
    logger.info(s"abort[$writerId]")
  }

  override def close(): Unit = {
    logger.info(s"close[$writerId]")
  }

  ////////////////////////////
  // Private Helper Methods //
  ////////////////////////////

  private def sparkRecordBufferToKernelColumnarBatchIter(
      partitionArrayData: Array[InternalRow]): CloseableIterator[FilteredColumnarBatch] = {
    logger.info(s"arrayData: ${partitionArrayData.map(_.toString).mkString(", ")}")
    logger.info(s"arrayData: ${partitionArrayData.map(_.toString).mkString(", ")}")
    val numColumns = targetTableSchemaButNotTheWriteSchema.length()
    val size = partitionArrayData.length

    logger.info(
      s"sparkRecordBufferToKernelColumnarBatchIter :: numColumns = $numColumns, size = $size")

    val columnVectors = new Array[ColumnVector](numColumns)

    for (i <- 0 until numColumns) {
      columnVectors(i) = targetTableSchemaButNotTheWriteSchema.at(i).getDataType match {
        case x: IntegerType =>
          new AbstractVectorWrapper(x, partitionArrayData, colIdx = i) {
            logger.info(s"Created IntegerType Vector Wrapper: coldIdx = $colIdx")

            override def getInt(rowId: Int): Int = {
              logger.info(s"Integer Vector Wrapper: getInt: rowId = $rowId, colIdx = $colIdx")
              checkValidRowId(rowId)
              bufferReference(rowId).getInt(colIdx)
            }
          }

        case x: StringType =>
          new AbstractVectorWrapper(x, partitionArrayData, colIdx = i) {
            logger.info(s"Created StringType Vector Wrapper: coldIdx = $colIdx")

            override def getString(rowId: Int): String = {
              logger.info(s"String Vector Wrapper: getString: rowId = $rowId, colIdx = $colIdx")
              checkValidRowId(rowId)
              bufferReference(rowId).getUTF8String(colIdx).toString
            }
          }

        case x: BooleanType =>
          new AbstractVectorWrapper(x, partitionArrayData, colIdx = i) {
            logger.info(s"Created BooleanType Vector Wrapper: coldIdx = $colIdx")

            override def getBoolean(rowId: Int): Boolean = {
              logger.info(s"Boolean Vector Wrapper: getBoolean: rowId = $rowId, colIdx = $colIdx")
              checkValidRowId(rowId)
              bufferReference(rowId).getBoolean(colIdx)
            }
          }

        case x: LongType =>
          new AbstractVectorWrapper(x, partitionArrayData, colIdx = i) {
            logger.info(s"Created LongType Vector Wrapper: coldIdx = $colIdx")

            override def getLong(rowId: Int): Long = {
              checkValidRowId(rowId)
              val ret = bufferReference(rowId).getLong(colIdx)
              logger.info(
                s"Long Vector Wrapper: getLong: rowId = $rowId, colIdx = $colIdx, ret = $ret")
              ret
            }
          }

        case x =>
          throw new UnsupportedOperationException(s"Unsupported data type: $x")
      }
    }

    logger.info(s"sparkRecordBufferToKernelColumnarBatchIter :: columnVectors $columnVectors")

    val filteredColumnarBatch = new FilteredColumnarBatch(
      new DefaultColumnarBatch(size, targetTableSchemaButNotTheWriteSchema, columnVectors),
      java.util.Optional.empty() /* selectionVector */
    );

    io.delta.kernel.internal.util.Utils.singletonCloseableIterator(filteredColumnarBatch)
  }

  private def closeableIteratorToArray[T: ClassTag](iterator: CloseableIterator[T]): Array[T] = {
    val buffer = scala.collection.mutable.ArrayBuffer.empty[T]

    try {
      while (iterator.hasNext) {
        buffer += iterator.next()
      }
    } finally {
      iterator.close() // Ensures the resource is closed
    }

    buffer.toArray[T]
  }
}

object SparkBatchDataWriter {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
}

///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////

/** Serialized and sent from the Executor to the Driver */
class SparkWriterCommitMessage(val serializedActions: Array[String])
    extends WriterCommitMessage
    with Serializable
