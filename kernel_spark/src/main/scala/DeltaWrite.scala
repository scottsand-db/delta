import io.delta.kernel.defaults.internal.json.JsonUtils
import io.delta.kernel.{Operation, Table => KernelTable}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, LogicalWriteInfo, PhysicalWriteInfo, Write, WriteBuilder, WriterCommitMessage}

class DeltaWriteBuilder(kernelTable: KernelTable, logicalWriteInfo: LogicalWriteInfo)
    extends WriteBuilder {
  override def build(): Write = {
    // TODO: validate Spark schema is compatible with the Delta table: logicalWriteInfo.schema()
    new DeltaWrite(kernelTable, logicalWriteInfo)
  }
}

class DeltaWrite(kernelTable: KernelTable, logicalWriteInfo: LogicalWriteInfo) extends Write {
  override def toBatch: BatchWrite = new DeltaBatchWrite(kernelTable, logicalWriteInfo)
}

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
private class DeltaBatchWrite(kernelTable: KernelTable, logicalWriteInfo: LogicalWriteInfo)
    extends BatchWrite {
  private val engine =
    io.delta.kernel.defaults.engine.DefaultEngine.create(new Configuration())

  private val dataSourceSchema =
    SchemaUtils.convertSparkSchemaToKernelSchema(logicalWriteInfo.schema())

  private val txn = kernelTable
    .createTransactionBuilder(engine, "kernel-spark-dsv2", Operation.WRITE)
    .withSchema(engine, dataSourceSchema)
    .build(engine)

  val txnStateRowSerialized = JsonUtils.rowToJson(txn.getTransactionState(engine))

  /** Creates a writer factory which will be serialized and sent to executors. */
  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    new DeltaBatchWriterFactory()
  }

  /**
   * Commits this writing job with a list of commit messages. The commit messages are collected
   * from successful data writers and are produced by DataWriter.commit().
   */
  override def commit(messages: Array[WriterCommitMessage]): Unit = ???

  override def abort(messages: Array[WriterCommitMessage]): Unit = ???
}

/**
 * Note that, the writer factory will be serialized and sent to executors, then the data writer
 * will be created on executors and do the actual writing. So this interface must be serializable
 * and DataWriter doesn't need to be.
 */
private class DeltaBatchWriterFactory extends DataWriterFactory with Serializable {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = ???
}

private class DeltaBatchDataWriter extends DataWriter[InternalRow] {
  override def write(record: InternalRow): Unit = ???

  override def commit(): WriterCommitMessage = ???

  override def abort(): Unit = ???

  override def close(): Unit = ???
}
