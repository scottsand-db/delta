/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.internal;

import static io.delta.kernel.internal.TableConfig.*;
import static io.delta.kernel.internal.TableConfig.TOMBSTONE_RETENTION;

import io.delta.kernel.ScanBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.CommitCoordinatorClientHandler;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.CommitInfo;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.replay.CreateCheckpointIterator;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.snapshot.TableCommitCoordinatorClientHandler;
import io.delta.kernel.types.StructType;
import java.util.Optional;

/** Implementation of {@link Snapshot}. */
public class SnapshotImpl implements Snapshot {
  private final Path logPath;
  private final Path dataPath;
  private final long version;
  private final LogReplay logReplay;
  private final Protocol protocol;
  private final Metadata metadata;
  private final LogSegment logSegment;
  private Optional<Long> inCommitTimestampOpt;

  public SnapshotImpl(
      Path dataPath,
      LogSegment logSegment,
      LogReplay logReplay,
      Protocol protocol,
      Metadata metadata) {
    this.logPath = new Path(dataPath, "_delta_log");
    this.dataPath = dataPath;
    this.version = logSegment.version;
    this.logSegment = logSegment;
    this.logReplay = logReplay;
    this.protocol = protocol;
    this.metadata = metadata;
    this.inCommitTimestampOpt = Optional.empty();
  }

  /////////////////
  // Public APIs //
  /////////////////

  @Override
  public long getVersion(Engine engine) {
    return version;
  }

  @Override
  public long getTimestamp(Engine engine) {
    if (TableConfig.isICTEnabled(engine, metadata)) {
      if (!inCommitTimestampOpt.isPresent()) {
        Optional<CommitInfo> commitInfoOpt =
            CommitInfo.getCommitInfoOpt(engine, logPath, logSegment.version);
        inCommitTimestampOpt =
            Optional.of(
                CommitInfo.getRequiredInCommitTimestamp(
                    commitInfoOpt, String.valueOf(logSegment.version), dataPath));
      }
      return inCommitTimestampOpt.get();
    } else {
      return logSegment.lastCommitTimestamp;
    }
  }

  @Override
  public StructType getSchema(Engine engine) {
    return getMetadata().getSchema();
  }

  @Override
  public ScanBuilder getScanBuilder(Engine engine) {
    return new ScanBuilderImpl(dataPath, protocol, metadata, getSchema(engine), logReplay, engine);
  }

  ///////////////////
  // Internal APIs //
  ///////////////////

  public Path getLogPath() {
    return logPath;
  }

  public Path getDataPath() {
    return dataPath;
  }

  public Protocol getProtocol() {
    return protocol;
  }

  public Metadata getMetadata() {
    return metadata;
  }

  public LogSegment getLogSegment() {
    return logSegment;
  }

  public CreateCheckpointIterator getCreateCheckpointIterator(Engine engine) {
    long minFileRetentionTimestampMillis =
        System.currentTimeMillis() - TOMBSTONE_RETENTION.fromMetadata(engine, metadata);
    return new CreateCheckpointIterator(engine, logSegment, minFileRetentionTimestampMillis);
  }

  /**
   * Get the latest transaction version for given <i>applicationId</i>. This information comes from
   * the transactions identifiers stored in Delta transaction log. This API is not a public API. For
   * now keep this internal to enable Flink upgrade to use Kernel.
   *
   * @param applicationId Identifier of the application that put transaction identifiers in Delta
   *     transaction log
   * @return Last transaction version or {@link Optional#empty()} if no transaction identifier
   *     exists for this application.
   */
  public Optional<Long> getLatestTransactionVersion(Engine engine, String applicationId) {
    return logReplay.getLatestTransactionIdentifier(engine, applicationId);
  }

  /**
   * Returns the commit coordinator client handler based on the table metadata in this snapshot.
   *
   * @param engine the engine to use for IO operations
   * @return the commit coordinator client handler for this snapshot or empty if the metadata is not
   *     configured to use the commit coordinator.
   */
  public Optional<TableCommitCoordinatorClientHandler> getTableCommitCoordinatorClientHandlerOpt(
      Engine engine) {
    return COORDINATED_COMMITS_COORDINATOR_NAME
        .fromMetadata(engine, metadata)
        .map(
            commitCoordinatorStr -> {
              CommitCoordinatorClientHandler handler =
                  engine.getCommitCoordinatorClientHandler(
                      commitCoordinatorStr,
                      COORDINATED_COMMITS_COORDINATOR_CONF.fromMetadata(engine, metadata));
              return new TableCommitCoordinatorClientHandler(
                  handler,
                  logPath.toString(),
                  COORDINATED_COMMITS_TABLE_CONF.fromMetadata(engine, metadata));
            });
  }
}
