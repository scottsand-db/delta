/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package io.delta.kernel.ccv2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.utils.FileStatus;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public abstract class BagOfPropertiesResolvedMetadata implements ResolvedMetadata {

  //////////////////////
  // static constants //
  //////////////////////

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static final String PREFIX = "__delta__.";

  /** __delta__.path */
  public static final String PATH_KEY = PREFIX + "path";

  /** __delta__.version */
  public static final String VERSION_KEY = PREFIX + "version";

  /** __delta__.__tracked_commit_files__.$filePath=json-encoded SIZE and MODIFICATION_TIME */
  public static final String CATALOG_TRACKED_COMMIT_FILES_PREFIX =
      PREFIX + "__tracked_commit_files__.";

  // TODO: flatten this out
  /** __delta__.protocol */
  public static final String PROTOCOL_KEY = PREFIX + "protocol";

  // TODO: flatten this out
  /** __delta__.metadata */
  public static final String METADATA_KEY = PREFIX + "metadata";

  ////////////////////////
  // instance variables //
  ////////////////////////

  private List<Tuple2<String, String>> propertiesList;
  private Map<String, String> propertiesMap;

  public BagOfPropertiesResolvedMetadata() {
    this.propertiesList = null;
    this.propertiesMap = null;
  }

  // TODO: this is janky / hacky
  public void initialize(List<Tuple2<String, String>> propertiesList) {
    this.propertiesList = propertiesList;
    this.propertiesMap = propertiesList.stream().collect(Collectors.toMap(x -> x._1, x -> x._2));
  }

  ////////////////////////////////
  // ResolvedMetadata Overrides //
  ////////////////////////////////

  @Override
  public String getPath() {
    return getOrThrow(PATH_KEY);
  }

  @Override
  public long getVersion() {
    return Long.parseLong(getOrThrow(VERSION_KEY));
  }

  @Override
  public Optional<LogSegment> getLogSegment() {
    return Optional.of(
        new LogSegment(
            new Path(getPath(), "_delta_log"),
            getVersion(),
            getCatalogTrackedFileStatuses() /* deltas */,
            Collections.emptyList(),
            100));
  }

  @Override
  public Optional<Protocol> getProtocol() {
    if (propertiesMap.containsKey(PROTOCOL_KEY)) {
      return Optional.of(Protocol.fromJson(getOrThrow(PROTOCOL_KEY)));
    }

    return Optional.empty();
  }

  // TODO: JSON serializing the entire metadata will make small updates to large schemas
  //       unnecessarily expensive
  @Override
  public Optional<Metadata> getMetadata() {
    if (propertiesMap.containsKey(METADATA_KEY)) {
      return Optional.of(Metadata.fromJson(getOrThrow(METADATA_KEY)));
    }

    return Optional.empty();
  }

  @Override
  public Optional<String> getSchemaString() {
    return getMetadata().map(Metadata::getSchemaString);
  }

  // ===== Note: commit is *not* implemented ===== //

  private String getOrThrow(String key) {
    return propertiesMap.computeIfAbsent(
        key,
        k -> {
          throw new RuntimeException(String.format("%s not found", k));
        });
  }

  private List<FileStatus> getCatalogTrackedFileStatuses() {
    return propertiesMap.entrySet().stream()
        .filter(entry -> entry.getKey().startsWith(CATALOG_TRACKED_COMMIT_FILES_PREFIX))
        .map(
            entry -> {
              try {
                final String filePath =
                    entry.getKey().substring(CATALOG_TRACKED_COMMIT_FILES_PREFIX.length());
                final JsonNode jsonNode = OBJECT_MAPPER.readTree(entry.getValue());
                final long size = jsonNode.get("size").asLong();
                final long modificationTime = jsonNode.get("modificationTime").asLong();
                return FileStatus.of(filePath, size, modificationTime);
              } catch (Exception ex) {
                throw new RuntimeException(
                    String.format(
                        "Failed to parse JSON entry %s -> %s", entry.getKey(), entry.getValue()),
                    ex);
              }
            })
        .sorted(Comparator.comparing(FileStatus::getPath))
        .collect(Collectors.toList());
  }
}
