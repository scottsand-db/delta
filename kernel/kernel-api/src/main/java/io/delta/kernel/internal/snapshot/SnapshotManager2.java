package io.delta.kernel.internal.snapshot;

import static java.lang.String.format;

import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.DeltaErrors;
import io.delta.kernel.internal.checkpoints.CheckpointInstance;
import io.delta.kernel.internal.checkpoints.CheckpointMetaData;
import io.delta.kernel.internal.checkpoints.Checkpointer;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.lang.ListUtils;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.util.KernelLogger;
import io.delta.kernel.internal.util.TimedLogger;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.utils.FileStatus;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.LoggerFactory;

public class SnapshotManager2 {

  private static final KernelLogger logger =
      new KernelLogger(LoggerFactory.getLogger(SnapshotManager2.class));

  private final Path dataPath;
  private final Path logPath;
  private final TimedLogger timedLogger;
  private final LogFileLister logFileLister;
  private final Checkpointer checkpointer;

  public SnapshotManager2(Path dataPath, Path logPath) {
    this.dataPath = dataPath;
    this.logPath = logPath;

    this.timedLogger = new TimedLogger(dataPath.toString(), logger);
    this.logFileLister = new LogFileLister(dataPath, logPath);
    this.checkpointer = new Checkpointer(logPath);
  }

  /////////////////
  // Public APIs //
  /////////////////

  public Snapshot loadLatestSnapshot() {
    return null;
  }

  public Snapshot loadSnapshotAtVersion(long version) {
    return null;
  }

  public Snapshot loadSnapshotAtTimestamp(long timestamp) {
    return null;
  }

  ////////////////////////////
  // Private Helper Methods //
  ////////////////////////////

  private LogSegment getLogSegment(Engine engine, Optional<Long> versionToLoad) {
    ////////////////////////////////////////////////////////////////////////////////////////////////
    // Step 1a: Find the latest checkpoint version. If $versionToLoad is empty, use the version   //
    //          referenced by the _LAST_CHECKPOINT file. If $versionToLoad is present, search for //
    //          the previous latest complete checkpoint at or before $versionToLoad.              //
    ////////////////////////////////////////////////////////////////////////////////////////////////

    final Optional<Long> startCheckpointVersion;
    if (versionToLoad.isPresent()) {
      startCheckpointVersion = timedLogger.timeOperation(
          String.format("Load last checkpoint at or before version %s", versionToLoad),
          () -> Checkpointer
              .findLastCompleteCheckpointBefore(engine, logPath, versionToLoad.get() + 1)
              .map(x -> x.version)
      );
    } else {
      startCheckpointVersion =
          checkpointer.readLastCheckpointFile(engine).map(CheckpointMetaData::getVersion);
    }

    //////////////////////////////////////////////////////////////////
    // Step 1b: Determine the actual version to start loading from. //
    //////////////////////////////////////////////////////////////////

    final long startVersion = startCheckpointVersion.orElseGet(() -> {
      logger.warn("[{}]: Cannot find a complete checkpoint. Listing from version 0", dataPath);
      return 0L;
    });

    // Step 2: List the files from $startVersion to $versionToLoad
    final Optional<List<FileStatus>> newFileStatusesOpt = timedLogger.timeOperation(
        String.format(
            "List the files from startVersion=%s to endVersion=%s", startVersion, versionToLoad),
        () -> logFileLister.listDeltaAndCheckpointFiles(engine, startVersion, versionToLoad)
    );

    /////////////////////////////////////////////
    // Step 3: Perform some basic validations. //
    /////////////////////////////////////////////

    if (!newFileStatusesOpt.isPresent()) {
      if (!startCheckpointVersion.isPresent()) {
        // No files found even when listing from 0 => empty directory => table does not exist.
        throw new TableNotFoundException(dataPath.toString());
      } else {
        // There are no files present at all in the directory yet, previously, we had found a valid
        // $startCheckpointVersion. The directory may have been deleted.
        throw DeltaErrors.inconsistentDeltaLog(dataPath.toString());
      }
    } else if (newFileStatusesOpt.get().isEmpty()) {
      if (!startCheckpointVersion.isPresent()) {
        // We can't construct a Snapshot because the directory contained no usable commit files...
        // Note that in this case the directory was not empty.
        throw new RuntimeException(
            String.format("No delta files found in the directory: %s", logPath)
        );
      } else {
        // There are some files present in the directory but no useful Delta files. But previously
        // we had found a valid $startCheckpointVersion. The directory may have been tampered with.
        throw DeltaErrors.inconsistentDeltaLog(dataPath.toString());
      }
    }

    final List<FileStatus> newFileStatuses = newFileStatusesOpt.get();

    // TODO: logger.debug $newFileStatuses

    /////////////////////////////////////////////////////////////////////////
    // Step 4: Partition $newFileStatuses into the checkpoints and deltas. //
    /////////////////////////////////////////////////////////////////////////

    final Tuple2<List<FileStatus>, List<FileStatus>> checkpointAndDeltaFileStatuses =
        ListUtils.partition(
            newFileStatuses,
            fileStatus -> FileNames.isCheckpointFile(new Path(fileStatus.getPath()).getName()));
    final List<FileStatus> checkpointFileStatuses = checkpointAndDeltaFileStatuses._1;
    final List<FileStatus> deltaFileStatuses = checkpointAndDeltaFileStatuses._2;

    // TODO: logger.debug $checkpointFileStatuses
    // TODO: logger.debug $deltasFileStatuses

    ///////////////////////////////////////////////////////////////
    // Step 5: Determine the latest complete checkpoint version. //
    ///////////////////////////////////////////////////////////////

    final List<CheckpointInstance> checkpointInstances = checkpointFileStatuses
        .stream()
        .map(f -> new CheckpointInstance(f.getPath()))
        .collect(Collectors.toList());

    final CheckpointInstance notLaterThanCheckpoint =
        versionToLoad.map(CheckpointInstance::new).orElse(CheckpointInstance.MAX_VALUE);

    final Optional<CheckpointInstance> latestCompleteCheckpoint =
        Checkpointer.getLatestCompleteCheckpointFromList(checkpointInstances,
            notLaterThanCheckpoint);

    if (!latestCompleteCheckpoint.isPresent() && startCheckpointVersion.isPresent()) {
      // In Step 1a we found the $startCheckpointVersion but now our LIST of the file system doesn't
      // see it. This means that the checkpoint we thought should exist no longer does.
      throw DeltaErrors.inconsistentDeltaLog(dataPath.toString());
    }

    final long latestCompleteCheckpointVersion =
        latestCompleteCheckpoint.map(x -> x.version).orElse(-1L);

    //////////////////////////////////////////////////////////////////////////////
    // Step 6: Grab all the deltas newer than $latestCompleteCheckpointVersion. //
    //////////////////////////////////////////////////////////////////////////////

    final List<Tuple2<FileStatus, Long>> deltasAndVersionsAfterCheckpoint = deltaFileStatuses
        .stream()
        .map(fs -> new Tuple2<>(fs, FileNames.deltaVersion(new Path(fs.getPath()))))
        .filter(x -> x._2 > latestCompleteCheckpointVersion)
        .collect(Collectors.toList());

    final List<FileStatus> deltasAfterCheckpoint =
        deltasAndVersionsAfterCheckpoint.stream().map(x -> x._1).collect(Collectors.toList());

    final List<Long> deltaVersionsAfterCheckpoint =
        deltasAndVersionsAfterCheckpoint.stream().map(x -> x._2).collect(Collectors.toList());

    // TODO: logger.debug $deltasAfterCheckpoint

    ///////////////////////////////////////////////////////////////////
    // Step 7: Determine the version of the snapshot we can now load //
    ///////////////////////////////////////////////////////////////////

    final long newVersion = deltaVersionsAfterCheckpoint.isEmpty()
        ? latestCompleteCheckpointVersion
        : deltaVersionsAfterCheckpoint.get(deltaVersionsAfterCheckpoint.size() - 1);

  }
}
