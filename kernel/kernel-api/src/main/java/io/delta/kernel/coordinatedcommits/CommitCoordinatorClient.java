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

package io.delta.kernel.coordinatedcommits;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.coordinatedcommits.actions.AbstractMetadata;
import io.delta.kernel.coordinatedcommits.actions.AbstractProtocol;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;

import io.delta.kernel.utils.CloseableIterator;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.util.Map;

@Evolving
public interface CommitCoordinatorClient {
  Map<String, String> registerTable(
      Engine engine,
      String logPath,
      @Nullable TableIdentifier tableIdentifier,
      long currentVersion,
      AbstractMetadata currentMetadata,
      AbstractProtocol currentProtocol);

  CommitResponse commit(
      Engine engine,
      TableDescriptor tableDescriptor,
      long commitVersion,
      CloseableIterator<Row> actions,
      UpdatedActions updatedActions)
      throws CommitFailedException;

  GetCommitsResponse getCommits(
      Engine engine,
      TableDescriptor tableDescriptor,
      @Nullable Long startVersion,
      @Nullable Long endVersion);

  void backfillToVersion(
      Engine engine,
      TableDescriptor tableDescriptor,
      long version,
      @Nullable Long lastKnownBackfilledVersion)
      throws IOException;

  boolean semanticEquals(CommitCoordinatorClient other);
}
