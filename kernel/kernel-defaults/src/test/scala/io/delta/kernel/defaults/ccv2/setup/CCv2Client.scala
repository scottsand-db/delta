package io.delta.kernel.defaults.ccv2.setup

import java.util.{Collections, UUID, Iterator => IteratorJ}

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import io.delta.kernel.ccv2.{BagOfPropertiesResolvedMetadata, CommitResult, ResolvedMetadata}
import io.delta.kernel.data.Row
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.{FileNames, Tuple2 => Tuple2J}
import io.delta.kernel.utils.{CloseableIterator, FileStatus}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path => HadoopPath}
import org.slf4j.Logger

class CCv2Client(engine: Engine, catalogClient: CatalogClient) {
  def getResolvedMetadata(tableName: String): ResolvedMetadata = {
    new ResolvedCatalogMetadata(tableName, engine, catalogClient)
  }

  def getStagingTableResolvedMetadata(
      tableName: String,
      engine: Engine,
      catalogClient: CatalogClient): ResolvedMetadata = {
    new StagingCatalogResolvedMetadata(tableName, engine, catalogClient)
  }

}

//////////////////////////////////////
// ResolvedCatalogMetadataCommitter //
//////////////////////////////////////

trait ResolvedCatalogMetadataCommitter extends { self: BagOfPropertiesResolvedMetadata =>
  import ResolvedCatalogMetadataCommitter._
  import BagOfPropertiesResolvedMetadata._
  import JavaScalaUtils._

  // lazy so the child can finish initializing before we use `getPath`
  private lazy val logPath = new Path(getPath, "_delta_log")
  private lazy val hadoopFileSystem = FileSystem.getLocal(new Configuration())
  
  def engine: Engine
  def catalogClient: CatalogClient
  def logger: Logger
  def tableName: String

  override def commit(
      commitAsVersion: Long,
      finalizedActions: CloseableIterator[Row],
      metaInfo: IteratorJ[Tuple2J[String, String]]): CommitResult = {
    val metaInfoList = metaInfo.asScala.toList // we want to print it out + use it multiple times
    val logPath = s"$getPath/_delta_log"
    val uuidCommitsPath = s"$logPath/_commits"
    val commitFilePath =
      f"$uuidCommitsPath/$commitAsVersion%020d.${UUID.randomUUID().toString}.json"

    logger.info(s"tableName: $tableName")
    logger.info(s"dataPath: $getPath")
    logger.info(s"commitAsVersion: $commitAsVersion")
    logger.info(s"commitFilePath: $commitFilePath")
    logger.info(s"metaInfo: ${metaInfoList.map(x => (x._1, x._2)).mkString(", ")}")

    logger.info("Write UUID commit file: START")
    engine
      .getJsonHandler
      .writeJsonFileAtomically(commitFilePath, finalizedActions, false /* overwrite */)
    logger.info("Write UUID commit file: END")

    val hadoopFs = hadoopFileSystem.getFileStatus(new HadoopPath(commitFilePath))

    val kernelFs =
      FileStatus.of(hadoopFs.getPath.toString, hadoopFs.getLen, hadoopFs.getModificationTime)

    val fsMapData = Map(
      "size" -> hadoopFs.getLen.toString,
      "modificationTime" -> hadoopFs.getModificationTime.toString)

    val commitKey = CATALOG_TRACKED_COMMIT_FILES_PREFIX +
      hadoopFs.getPath.toString
    val commitJsonValue = OBJECT_MAPPER.writeValueAsString(fsMapData.asJava)
    val requirement = Requirement(
      name = "commitAsVersion",
      f = latestProperties => {
        val latestVersion = latestProperties.getOrElse(VERSION_KEY, "-1").toLong

        latestVersion == commitAsVersion - 1
      }
    )

    var allProperties = List((commitKey, commitJsonValue)) ++
      metaInfoList.map(x => (x._1, x._2))

    if (commitAsVersion == 0) {
      allProperties = allProperties :+ (PATH_KEY, getPath)
    }

    logger.info(s"hadoopFS: $hadoopFs")
    logger.info(s"kernelFs: $kernelFs")
    logger.info(s"File status Entry: $commitKey -> $commitJsonValue")
    logger.info(s"allProperties to write to catalog:\n${allProperties.mkString("\n")}")
    logger.info("Commit to catalog: START")

    val result = catalogClient
      .setProperties(tableName, allProperties, List(requirement)) match {
      case SetPropertiesResponse.Success =>
        logger.info("Commit to catalog: SUCCESS")
        new CommitResult.Success {
          override def getCommitAttemptVersion: Long = commitAsVersion
        }
      case SetPropertiesResponse.TableDoesNotExist =>
        logger.info("Commit to catalog: TABLE DOES NOT EXIST")
        new CommitResult.NonRetryableFailure {
          override def getMessage: String = s"Table $tableName does not exist"

          override def getCommitAttemptVersion: Long = commitAsVersion
        }
      case SetPropertiesResponse.RequirementFailed(
      requirement, latestProperties: List[(String, String)]) =>
        logger.info(s"Commit to catalog: REQUIREMENT FAILED ${requirement.name}")
        new CommitResult.RetryableFailure {
          override def getMessage: String = s"Requirement failed: ${requirement.name}"

          override def getCommitAttemptVersion: Long = commitAsVersion

          override def properties(): java.util.List[Tuple2J[String, String]] = {
            latestProperties.map { case (k, v) => new Tuple2J(k, v) }.asJava
          }
        }
    }

    try {
      if (commitAsVersion % 5 == 0) {
        backfill(commitAsVersion, kernelFs)
      } else {
        logger.info("Skipping backfill")
      }
    } catch {
      case e: Throwable => logger.warn("Backfill failed, ignoring", e)
    }

    result
  }

  private def backfill(commitAsVersion: Long, committedFileStatus: FileStatus): Unit = {
    logger.info(s"Backfilling: START. commitAsVersion=$commitAsVersion")
    val allCandidateUnbackfilledFilePaths = Seq(committedFileStatus.getPath) ++ propertiesMap
      .asScala
      .filter { case (k, _) => k.startsWith(CATALOG_TRACKED_COMMIT_FILES_PREFIX) }
      .map { case (k, _) => k.stripPrefix(CATALOG_TRACKED_COMMIT_FILES_PREFIX) }

    logger.info(s"allCandidateUnbackfilledFilePaths: $allCandidateUnbackfilledFilePaths")

    allCandidateUnbackfilledFilePaths
      // e.g. perhaps some of the deltas we got back from the catalog were in fact backfilled
      .filter(path => FileNames.isUnbackfilledDeltaFile(path))
      .foreach { path =>
        val fsVersion = FileNames.uuidCommitDeltaVersion(path)
        val backfilledFilePath = FileNames.deltaFile(logPath, fsVersion)
        logger.info(s"Unbackfilled fs: ${path}")
        logger.info(s"Unbackfilled version: $fsVersion")
        logger.info(s"Backfilled file path: $backfilledFilePath")

        if (hadoopFileSystem.exists(new HadoopPath(backfilledFilePath))) {
          logger.info(s"Backfilled file already exists: $backfilledFilePath")
        } else {
          logger.info(s"Backfilling: $backfilledFilePath")
          val sourceUnbackfilledPath = new HadoopPath(path)
          val targetBackfilledPath = new HadoopPath(backfilledFilePath)
          logger.info(s"Copying $sourceUnbackfilledPath to $targetBackfilledPath")

          // TODO: This is not atomic btw
          FileUtil.copy(
            hadoopFileSystem, // sourceFileSystem
            sourceUnbackfilledPath, // sourcePath
            hadoopFileSystem, // targetFileSystem
            targetBackfilledPath, // targetPath
            false, // deleteSource
            false, // overwrite
            hadoopFileSystem.getConf)
        }
      }

    logger.info(s"Invoking catalog with latest backfilled version: $commitAsVersion")
    val backfillProperties = allCandidateUnbackfilledFilePaths.map { path =>
      (CATALOG_TRACKED_COMMIT_FILES_PREFIX + path, null)
    }.toList
    logger.info(s"backfillProperties: $backfillProperties")
    catalogClient
      .setProperties(tableName, backfillProperties)
    logger.info("Backfilling: END")
  }
}

object ResolvedCatalogMetadataCommitter {
  // create object mapper
  private val OBJECT_MAPPER = new ObjectMapper;

}

////////////////////////////////////
// StagingCatalogResolvedMetadata //
////////////////////////////////////

class StagingCatalogResolvedMetadata(
    override val tableName: String,
    override val engine: Engine,
    override val catalogClient: CatalogClient)
  extends BagOfPropertiesResolvedMetadata with ResolvedCatalogMetadataCommitter {

  import BagOfPropertiesResolvedMetadata._
  import StagingCatalogResolvedMetadata._

  catalogClient.createStagingTable(tableName) match {
    case CreateStagingTableResponse.Success(path) =>
      super.initialize(
        Seq(
          new Tuple2J(PATH_KEY, path),
          new Tuple2J(VERSION_KEY, "-1"),
        ).toList.asJava
      )
    case CreateStagingTableResponse.TableAlreadyExists =>
      throw new RuntimeException(s"Table $tableName already exists")
  }

  // ===== ResolvedCatalogMetadataCommitter overrides ===== //

  override def logger: Logger = _logger
}

object StagingCatalogResolvedMetadata {
  private val _logger = org.slf4j.LoggerFactory.getLogger(classOf[StagingCatalogResolvedMetadata])
}

/////////////////////////////
// ResolvedCatalogMetadata //
/////////////////////////////

class ResolvedCatalogMetadata(
    override val tableName: String,
    override val engine: Engine,
    override val catalogClient: CatalogClient)
  extends BagOfPropertiesResolvedMetadata with ResolvedCatalogMetadataCommitter {

  import JavaScalaUtils._
  import ResolvedCatalogMetadata._

  catalogClient.getProperties(tableName) match {
    case GetPropertiesResponse.Success(properties) =>
      super.initialize(properties.map { case (k, v) => new Tuple2J(k, v) }.asJava)
    case GetPropertiesResponse.TableDoesNotExist =>
      throw new RuntimeException(s"Table $tableName does not exists")
  }

  // ===== ResolvedCatalogMetadataCommitter overrides ===== //

  override def logger: Logger = _logger

}

object ResolvedCatalogMetadata {
  private val _logger = org.slf4j.LoggerFactory.getLogger(classOf[ResolvedCatalogMetadata])
}

