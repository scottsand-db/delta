/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.sharing.spark

import java.io.{ByteArrayInputStream, FileNotFoundException}
import java.net.{URI, URLDecoder, URLEncoder}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark.sql.delta.actions.{
  AddCDCFile,
  AddFile,
  DeletionVectorDescriptor,
  RemoveFile,
  SingleAction
}
import org.apache.spark.sql.delta.util.FileNames
import io.delta.sharing.client.util.JsonUtils
import io.delta.sharing.spark.DeltaSharingUtils.{
  DeltaSharingTableMetadata,
  FAKE_CHECKPOINT_BYTE_ARRAY
}
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.storage.{BlockId}

/** Read-only file system for delta sharing log.
 * This is a faked file system to serve data under path delta-sharing-log:/. The delta log will be
 * prepared by DeltaSharingDataSource and its related classes, put in blockManager, and then serve
 * to DeltaLog with a path pointing to this file system.
 * In executor, when it tries to read data from the delta log, this file system class will return
 * the data fetched from the block manager.
 */
private[sharing] class DeltaSharingLogFileSystem extends FileSystem with Logging {
  import DeltaSharingLogFileSystem._

  override def getScheme: String = SCHEME

  override def getUri(): URI = URI.create(s"$SCHEME:///")

  override def open(f: Path, bufferSize: Int): FSDataInputStream = {
    if (FileNames.isCheckpointFile(f)) {
      new FSDataInputStream(
        new SeekableByteArrayInputStream(DeltaSharingUtils.FAKE_CHECKPOINT_BYTE_ARRAY)
      )
    } else if (FileNames.isDeltaFile(f)) {
      val iterator =
        SparkEnv.get.blockManager.get[String](getDeltaSharingLogBlockId(f.toString)) match {
          case Some(block) => block.data.asInstanceOf[Iterator[String]]
          case _ => throw new FileNotFoundException(s"Cannot find block for delta log file: $f.")
        }
      // Explicitly call hasNext to allow the reader lock on the block to be released.
      val arrayBuilder = Array.newBuilder[Byte]
      while (iterator.hasNext) {
        val actionJsonStr = iterator.next()
        arrayBuilder ++= actionJsonStr.getBytes()
      }
      // We still have to load the full content of a delta log file in memory to serve them.
      // This still exposes the risk of OOM.
      new FSDataInputStream(new SeekableByteArrayInputStream(arrayBuilder.result()))
    } else {
      val content = getBlockAndReleaseLockHelper[String](f, None)
      new FSDataInputStream(new SeekableByteArrayInputStream(content.getBytes()))
    }
  }

  override def exists(f: Path): Boolean = {
    // The reason of using the variable exists is to allow us to explicitly release the reader lock
    // on the blockId.
    val blockId = getDeltaSharingLogBlockId(f.toString)
    val exists = SparkEnv.get.blockManager.get(blockId).isDefined
    if (exists) {
      releaseLockHelper(blockId)
    }
    exists
  }

  // Delta sharing log file system serves checkpoint file with a CONSTANT value so we construct the
  // FileStatus when the function is being called.
  // For other files, they will be constructed and put into block manager when constructing the
  // delta log based on the rpc response from the server.
  override def getFileStatus(f: Path): FileStatus = {
    val status = if (FileNames.isCheckpointFile(f)) {
      DeltaSharingLogFileStatus(
        path = f.toString,
        size = FAKE_CHECKPOINT_BYTE_ARRAY.size,
        modificationTime = 0L
      )
    } else {
      getBlockAndReleaseLockHelper[DeltaSharingLogFileStatus](f, Some("_status"))
    }

    new FileStatus(
      /* length */ status.size,
      /* isdir */ false,
      /* block_replication */ 0,
      /* blocksize */ 1,
      /* modification_time */ status.modificationTime,
      /* path */ new Path(status.path)
    )
  }

  /**
   * @param f: a Path pointing to a delta log directory of a delta sharing table, example:
   *            delta-sharing-log:/customized-delta-sharing-table/_delta_log
   *            The iterator contains a list of tuple(json_file_path, json_file_size) which are
   *            pre-prepared and set in the block manager by DeltaSharingDataSource and its related
   *            classes.
   * @return the list of json files under the /_delta_log directory, if prepared.
   */
  override def listStatus(f: Path): Array[FileStatus] = {
    val iterator =
      SparkEnv.get.blockManager
        .get[DeltaSharingLogFileStatus](getDeltaSharingLogBlockId(f.toString)) match {
        case Some(block) => block.data.asInstanceOf[Iterator[DeltaSharingLogFileStatus]]
        case _ => throw new FileNotFoundException(s"Failed to list files for path: $f.")
      }

    // Explicitly call hasNext to allow the reader lock on the block to be released.
    val arrayBuilder = Array.newBuilder[FileStatus]
    while (iterator.hasNext) {
      val fileStatus = iterator.next()
      arrayBuilder += new FileStatus(
        /* length */ fileStatus.size,
        /* isdir */ false,
        /* block_replication */ 0,
        /* blocksize */ 1,
        /* modification_time */ fileStatus.modificationTime,
        /* path */ new Path(fileStatus.path)
      )
    }
    arrayBuilder.result()
  }

  override def create(
      f: Path,
      permission: FsPermission,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable): FSDataOutputStream = {
    throw new UnsupportedOperationException(s"create: $f")
  }

  override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream = {
    throw new UnsupportedOperationException(s"append: $f")
  }

  override def rename(src: Path, dst: Path): Boolean = {
    throw new UnsupportedOperationException(s"rename: src:$src, dst:$dst")
  }

  override def delete(f: Path, recursive: Boolean): Boolean = {
    throw new UnsupportedOperationException(s"delete: $f")
  }
  override def listStatusIterator(f: Path): RemoteIterator[FileStatus] = {
    throw new UnsupportedOperationException(s"listStatusIterator: $f")
  }

  override def setWorkingDirectory(newDir: Path): Unit =
    throw new UnsupportedOperationException(s"setWorkingDirectory: $newDir")

  override def getWorkingDirectory: Path = new Path(getUri)

  override def mkdirs(f: Path, permission: FsPermission): Boolean = {
    throw new UnsupportedOperationException(s"mkdirs: $f")
  }

  override def close(): Unit = {
    super.close()
  }

  private def getBlockAndReleaseLockHelper[T: ClassTag](f: Path, suffix: Option[String]): T = {
    val blockId = getDeltaSharingLogBlockId(suffix.foldLeft(f.toString)(_ + _))
    val result = SparkEnv.get.blockManager.getSingle[T](blockId).getOrElse {
      throw new FileNotFoundException(f.toString)
    }
    releaseLockHelper(blockId)

    result
  }

  private def releaseLockHelper(blockId: BlockId): Unit = {
    try {
      SparkEnv.get.blockManager.releaseLock(blockId)
    } catch {
      // releaseLock may fail when the lock is not hold by this thread, we are not exactly sure
      // when it fails or not, but no need to fail the entire delta sharing query.
      case e: Throwable => logWarning(s"Error while releasing lock for blockId:$blockId: $e.")
    }
  }
}

/**
 * A case class including the metadata for the constructed delta log based on the delta sharing
 * rpc response.
 * @param idToUrl stores the id to url mapping, used to register to CachedTableManager
 * @param minUrlExpirationTimestamp used to indicate when to refresh urls in CachedTableManager
 * @param numFileActionsInMinVersionOpt This is needed because DeltaSource is not advancing the
 *                                      offset to the next version automatically when scanning
 *                                      through a snapshot, so DeltaSharingSource needs to count the
 *                                      number of files in the min version and advance the offset to
 *                                      the next version when the offset is at the last index of the
 *                                      version.
 * @param minVersion  minVersion of all the files returned from server
 * @param maxVersion  maxVersion of all the files returned from server
 */
case class ConstructedDeltaLogMetadata(
    idToUrl: Map[String, String],
    minUrlExpirationTimestamp: Option[Long],
    numFileActionsInMinVersionOpt: Option[Int],
    minVersion: Long,
    maxVersion: Long)

private[sharing] object DeltaSharingLogFileSystem extends Logging {

  val SCHEME = "delta-sharing-log"

  // The constant added as prefix to all delta sharing block ids.
  private val BLOCK_ID_TEST_PREFIX = "test_"

  // It starts with test_ to match the prefix of TestBlockId.
  // In the meantime, we'll investigate in an option to add a general purposed BlockId subclass
  // and use it in delta sharing.
  val DELTA_SHARING_LOG_BLOCK_ID_PREFIX = "test_delta-sharing-log:"

  def getDeltaSharingLogBlockId(path: String): BlockId = {
    BlockId(BLOCK_ID_TEST_PREFIX + path)
  }

  /**
   * Encode `tablePath` to a `Path` in the following format:
   *
   * ```
   * delta-sharing-log:///<url encoded table path>
   * ```
   *
   * This format can be decoded by `DeltaSharingLogFileSystem.decode`.
   * It will be used to:
   * 1) construct a DeltaLog class which points to a delta sharing table.
   * 2) construct a block id to look for commit files of the delta sharing table.
   */
  def encode(tablePath: String): Path = {
    val encodedTablePath = URLEncoder.encode(tablePath, "UTF-8")
    new Path(s"$SCHEME:///$encodedTablePath")
  }

  def decode(path: Path): String = {
    val encodedTablePath = path.toString
      .stripPrefix(s"$SCHEME:///")
      .stripPrefix(s"$SCHEME:/")
    URLDecoder.decode(encodedTablePath, "UTF-8")
  }

  // Convert a deletion vector path to a delta sharing path.
  // Only paths needs to be converted since it's pre-signed url. Inline DV should be handled
  // in place. And UUID should throw error since it should be converted to pre-signed url when
  // returned from the server.
  private def getDeltaSharingDeletionVectorDescriptor(
      fileAction: model.DeltaSharingFileAction,
      customTablePath: String): DeletionVectorDescriptor = {
    if (fileAction.getDeletionVectorOpt.isEmpty) {
      null
    } else {
      val deletionVector = fileAction.getDeletionVectorOpt.get
      deletionVector.storageType match {
        case DeletionVectorDescriptor.PATH_DV_MARKER =>
          deletionVector.copy(
            pathOrInlineDv = fileAction.getDeletionVectorDeltaSharingPath(customTablePath)
          )
        case DeletionVectorDescriptor.INLINE_DV_MARKER => deletionVector
        case storageType =>
          throw new IllegalStateException(
            s"Unexpected DV storage type:" +
            s"$storageType in the delta sharing response for ${fileAction.json}."
          )
      }
    }
  }

  // Only absolute path (which is pre-signed url) need to be put in IdToUrl mapping.
  // inline DV should be processed in place, and UUID should throw error.
  private def requiresIdToUrlForDV(deletionVectorOpt: Option[DeletionVectorDescriptor]): Boolean = {
    deletionVectorOpt.isDefined &&
    deletionVectorOpt.get.storageType == DeletionVectorDescriptor.PATH_DV_MARKER
  }

  /**
   * Convert DeltaSharingFileAction with delta sharing file path and serialize as json to store in
   * the delta log.
   *
   * @param fileAction               The DeltaSharingFileAction to convert.
   * @param customTablePath The table path used to construct action.path field.
   * @return json serialization of delta action.
   */
  private def getActionWithDeltaSharingPath(
      fileAction: model.DeltaSharingFileAction,
      customTablePath: String): String = {
    val deltaSharingPath = fileAction.getDeltaSharingPath(customTablePath)
    val newSingleAction = fileAction.deltaSingleAction.unwrap match {
      case add: AddFile =>
        add.copy(
          path = deltaSharingPath,
          deletionVector = getDeltaSharingDeletionVectorDescriptor(fileAction, customTablePath)
        )
      case cdc: AddCDCFile =>
        assert(
          cdc.deletionVector == null,
          "deletionVector not null in the AddCDCFile from delta" +
          s" sharing response: ${cdc.json}"
        )
        cdc.copy(path = deltaSharingPath)
      case remove: RemoveFile =>
        remove.copy(
          path = deltaSharingPath,
          deletionVector = getDeltaSharingDeletionVectorDescriptor(fileAction, customTablePath)
        )
      case action =>
        throw new IllegalStateException(
          s"unexpected action in delta sharing " +
          s"response: ${action.json}"
        )
    }
    newSingleAction.json
  }

  // Sort by id to keep a stable order of the files within a version in the delta log.
  private def deltaSharingFileActionIncreaseOrderFunc(
      f1: model.DeltaSharingFileAction,
      f2: model.DeltaSharingFileAction): Boolean = {
    f1.id < f2.id
  }


  /** Set the modificationTime to zero, this is to align with the time returned from
   * DeltaSharingFileSystem.getFileStatus
   */
  private def setModificationTimestampToZero(deltaSingleAction: SingleAction): SingleAction = {
    deltaSingleAction.unwrap match {
      case a: AddFile => a.copy(modificationTime = 0).wrap
      case _ => deltaSingleAction
    }
  }

  /**
   * Construct local delta log at version zero based on lines returned from delta sharing server,
   * to support latest snapshot or time travel queries. Storing both protocol/metadata and
   * the actual data actions in version 0 will simplify both the log construction and log reply.
   *
   * @param lines           a list of delta actions, to be processed and put in the local delta log,
   *                        each action contains a version field to indicate the version of log to
   *                        put it in.
   * @param customTablePath query customized table path, used to construct action.path field for
   *                        DeltaSharingFileSystem
   * @return ConstructedDeltaLogMetadata, which contains 3 fields:
   *          - idToUrl: mapping from file id to pre-signed url
   *          - minUrlExpirationTimestamp timestamp indicating the when to refresh pre-signed urls.
   *            Both are used to register to CachedTableManager.
   *          - maxVersion: to be 0.
   */
  def constructLocalDeltaLogAtVersionZero(
      lines: Seq[String],
      customTablePath: String): ConstructedDeltaLogMetadata = {
    val startTime = System.currentTimeMillis()
    val jsonLogSeq = Seq.newBuilder[String]
    var jsonLogSize = 0
    var minUrlExpirationTimestamp: Option[Long] = None
    val fileActionsSeq = ArrayBuffer[model.DeltaSharingFileAction]()
    val idToUrl = scala.collection.mutable.Map[String, String]()
    lines.foreach { line =>
      val action = JsonUtils.fromJson[model.DeltaSharingSingleAction](line).unwrap
      action match {
        case fileAction: model.DeltaSharingFileAction =>
          // Store file actions in an array to sort them based on id later.
          fileActionsSeq += fileAction.copy(
            deltaSingleAction = setModificationTimestampToZero(fileAction.deltaSingleAction)
          )
        case protocol: model.DeltaSharingProtocol =>
          val protocolJsonStr = protocol.deltaProtocol.json + "\n"
          jsonLogSize += protocolJsonStr.length
          jsonLogSeq += protocolJsonStr
        case metadata: model.DeltaSharingMetadata =>
          val metadataJsonStr = metadata.deltaMetadata.json + "\n"
          jsonLogSize += metadataJsonStr.length
          jsonLogSeq += metadataJsonStr
        case _ =>
          throw new IllegalStateException(
            s"unknown action in the delta sharing " +
            s"response: $line"
          )
      }
    }
    var previousIdOpt: Option[String] = None
    fileActionsSeq.toSeq.sortWith(deltaSharingFileActionIncreaseOrderFunc).foreach { fileAction =>
      assert(
        // Using > instead of >= because there can be a removeFile and addFile pointing to the same
        // parquet file which result in the same file id, since id is a hash of file path.
        // This is ok because eventually it can read data out of the correct parquet file.
        !previousIdOpt.exists(_ > fileAction.id),
        s"fileActions must be in increasing order by id: ${previousIdOpt} is not smaller than" +
        s" ${fileAction.id}."
      )
      previousIdOpt = Some(fileAction.id)

      // 1. build id to url mapping
      idToUrl(fileAction.id) = fileAction.path
      if (requiresIdToUrlForDV(fileAction.getDeletionVectorOpt)) {
        idToUrl(fileAction.deletionVectorFileId) =
          fileAction.getDeletionVectorOpt.get.pathOrInlineDv
      }

      // 2. prepare json log content.
      val actionJsonStr = getActionWithDeltaSharingPath(fileAction, customTablePath) + "\n"
      jsonLogSize += actionJsonStr.length
      jsonLogSeq += actionJsonStr

      // 3. process expiration timestamp
      if (fileAction.expirationTimestamp != null) {
        minUrlExpirationTimestamp =
          if (minUrlExpirationTimestamp.isDefined &&
            minUrlExpirationTimestamp.get < fileAction.expirationTimestamp) {
            minUrlExpirationTimestamp
          } else {
            Some(fileAction.expirationTimestamp)
          }
      }
    }

    val encodedTablePath = DeltaSharingLogFileSystem.encode(customTablePath)

    // Always use 0.json for snapshot queries.
    val deltaLogPath = s"${encodedTablePath.toString}/_delta_log"
    val jsonFilePath = FileNames.deltaFile(new Path(deltaLogPath), 0).toString
    DeltaSharingUtils.overrideIteratorBlock[String](
      getDeltaSharingLogBlockId(jsonFilePath),
      jsonLogSeq.result().toIterator
    )

    val fileStatusSeq = Seq(
      DeltaSharingLogFileStatus(path = jsonFilePath, size = jsonLogSize, modificationTime = 0L)
    )
    DeltaSharingUtils.overrideIteratorBlock[DeltaSharingLogFileStatus](
      getDeltaSharingLogBlockId(deltaLogPath),
      fileStatusSeq.toIterator
    )
    logInfo(
      s"It takes ${(System.currentTimeMillis() - startTime) / 1000.0}s to construct delta" +
      s" log for $customTablePath with ${idToUrl.toMap.size} urls."
    )
    ConstructedDeltaLogMetadata(
      idToUrl = idToUrl.toMap,
      minUrlExpirationTimestamp = minUrlExpirationTimestamp,
      numFileActionsInMinVersionOpt = None,
      minVersion = 0,
      maxVersion = 0
    )
  }

}

/**
 * A ByteArrayInputStream that implements interfaces required by FSDataInputStream, which is the
 * return type of DeltaSharingLogFileSystem.open. It will convert the string content as array of
 * bytes and allow caller to read data out of it.
 * The string content are list of json serializations of delta actions in a json delta log file.
 */
private[sharing] class SeekableByteArrayInputStream(bytes: Array[Byte])
    extends ByteArrayInputStream(bytes)
    with Seekable
    with PositionedReadable {
  assert(available == bytes.length)

  override def seek(pos: Long): Unit = {
    if (mark != 0) {
      throw new IllegalStateException("Cannot seek if mark is set")
    }
    reset()
    skip(pos)
  }

  override def seekToNewSource(pos: Long): Boolean = {
    false // there aren't multiple sources available
  }

  override def getPos(): Long = {
    bytes.length - available
  }

  override def read(buffer: Array[Byte], offset: Int, length: Int): Int = {
    super.read(buffer, offset, length)
  }

  override def read(pos: Long, buffer: Array[Byte], offset: Int, length: Int): Int = {
    if (pos >= bytes.length) {
      return -1
    }
    val readSize = math.min(length, bytes.length - pos).toInt
    System.arraycopy(bytes, pos.toInt, buffer, offset, readSize)
    readSize
  }

  override def readFully(pos: Long, buffer: Array[Byte], offset: Int, length: Int): Unit = {
    System.arraycopy(bytes, pos.toInt, buffer, offset, length)
  }

  override def readFully(pos: Long, buffer: Array[Byte]): Unit = {
    System.arraycopy(bytes, pos.toInt, buffer, 0, buffer.length)
  }
}

case class DeltaSharingLogFileStatus(path: String, size: Long, modificationTime: Long)
