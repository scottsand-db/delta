/*
 * Copyright (2024) The Delta Lake Project Authors.
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

package io.delta.kernel.internal.snapshot;

import io.delta.kernel.coordinatedcommits.CommitCoordinatorClient;
import io.delta.kernel.coordinatedcommits.CommitFailedException;
import io.delta.kernel.coordinatedcommits.CommitResponse;
import io.delta.kernel.coordinatedcommits.GetCommitsResponse;
import io.delta.kernel.coordinatedcommits.TableDescriptor;
import io.delta.kernel.coordinatedcommits.UpdatedActions;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.util.Map;

/**
 * A wrapper around {@link CommitCoordinatorClient} that provides a more user-friendly API
 * for committing/ accessing commits to a specific table. This class takes care of passing the table
 * specific configuration to the underlying {@link CommitCoordinatorClient} e.g. logPath /
 * coordinatedCommitsTableConf.
 */
public class TableCommitCoordinatorClientHandler {
  private final CommitCoordinatorClient commitCoordinatorClient;
  private final String logPath;
  private final Map<String, String> tableConf;
  private final TableDescriptor tableDescriptor;

  public TableCommitCoordinatorClientHandler(
      CommitCoordinatorClient commitCoordinatorClientHandler,
      String logPath,
      Map<String, String> tableConf) {
    this.commitCoordinatorClient = commitCoordinatorClientHandler;
    this.logPath = logPath;
    this.tableConf = tableConf;
    this.tableDescriptor = new TableDescriptor(logPath, null /* table identifier */, tableConf);
  }

  public CommitResponse commit(
      Engine engine,
      long commitVersion,
      CloseableIterator<Row> actions,
      UpdatedActions updatedActions)
      throws CommitFailedException {
    return commitCoordinatorClient.commit(
        engine, tableDescriptor, commitVersion, actions, updatedActions);
  }

  public GetCommitsResponse getCommits(Engine engine, Long startVersion, Long endVersion) {
    return commitCoordinatorClient.getCommits(engine, tableDescriptor, startVersion, endVersion);
  }

  public void backfillToVersion(Engine engine, long version, Long lastKnownBackfilledVersion)
      throws IOException {
    commitCoordinatorClient.backfillToVersion(
        engine, tableDescriptor, version, lastKnownBackfilledVersion);
  }

  public boolean semanticEquals(CommitCoordinatorClient otherCommitCoordinatorClientHandler) {
    return commitCoordinatorClient.semanticEquals(otherCommitCoordinatorClientHandler);
  }

  public boolean semanticEquals(TableCommitCoordinatorClientHandler otherCommitCoordinatorClient) {
    return semanticEquals(otherCommitCoordinatorClient.commitCoordinatorClient);
  }
}
