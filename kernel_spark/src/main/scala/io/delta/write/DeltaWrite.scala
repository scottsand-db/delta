package io.delta.write

import io.delta.kernel.{Table => KernelTable}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DeltaBatchWrite, DeltaWrite, DeltaWriteBuilder, DeltaWriter, DeltaWriterFactory, PhysicalWriteInfo, WriterCommitMessage}

///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////

class DeltaOfRowsWriteBuilder(kernelTable: KernelTable) extends DeltaWriteBuilder {
  override def build(): DeltaWrite = {
    new DeltaOfRowsWrite()
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////

class DeltaOfRowsWrite extends DeltaWrite {
  override def toBatch: DeltaBatchWrite = {
    new DeltaOfRowsBatchWrite()
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////

class DeltaOfRowsBatchWrite extends DeltaBatchWrite {
  override def createBatchWriterFactory(info: PhysicalWriteInfo): DeltaWriterFactory = {
    new DeltaOfRowsWriterFactory()
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {

  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {

  }
}

///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////

class DeltaOfRowsWriterFactory extends DeltaWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DeltaWriter[InternalRow] = {
    new DeltaOfRowsWriter()
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////

class DeltaOfRowsWriter extends DeltaWriter[InternalRow] {
  import DeltaOfRowsWriter._

  override def delete(metadata: InternalRow, id: InternalRow): Unit = {
    logger.info(s"Scott > DeltaOfRowsWriter > delete :: metadata=$metadata, id=$id")
  }

  override def update(metadata: InternalRow, id: InternalRow, row: InternalRow): Unit = {
    logger.info(s"Scott > DeltaOfRowsWriter > update :: metadata=$metadata, id=$id, row=$row")
  }

  override def insert(row: InternalRow): Unit = {
    logger.info(s"Scott > DeltaOfRowsWriter > insert :: row=$row")
  }

  override def commit(): WriterCommitMessage = {
    null
  }

  override def abort(): Unit = {

  }

  override def close(): Unit = {

  }
}

object DeltaOfRowsWriter {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
}
