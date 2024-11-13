/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.delta;

import io.delta.kernel.Operation;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.CloseableIterable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.LocationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeltaCatalog implements Catalog, Configurable<Configuration> {

  private static final Logger LOG = LoggerFactory.getLogger(DeltaCatalog.class);
  private static final String HIVE_WAREHOUSE_PROP = "hive.metastore.warehouse.dir";
  private static final Joiner SLASH = Joiner.on("/");

  private Configuration conf = null;
  private String name = null;
  private Map<String, String> catalogProperties = null;
  private String warehouse = null;
  private FileIO fileIO = null;

  @Override
  public String name() {
    return name;
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    return Collections.emptyList();
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    throw new UnsupportedOperationException("Drop is not supported");
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    throw new UnsupportedOperationException("Rename is not supported");
  }

  @Override
  public Table createTable(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> properties) {
    LOG.info(
        "createTable ::: identifier {} with schema {} and spec {} and location {} and properties {}",
        identifier,
        schema,
        spec,
        location,
        properties);
    return createTableHelper(identifier, schema, Optional.of(spec));
  }

  @Override
  public Table createTable(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      Map<String, String> properties) {
    LOG.info(
        "createTable ::: identifier {} with schema {} and spec {} and properties {}",
        identifier,
        schema,
        spec,
        properties);
    return createTableHelper(identifier, schema, Optional.of(spec));
  }

  @Override
  public Table createTable(TableIdentifier identifier, Schema schema, PartitionSpec spec) {
    LOG.info("createTable ::: identifier {} with schema {} and spec {}", identifier, schema, spec);
    return createTableHelper(identifier, schema, Optional.of(spec));
  }

  @Override
  public Table createTable(TableIdentifier identifier, Schema schema) {
    LOG.info("createTable ::: identifier {} with schema {}", identifier, schema);

    return createTableHelper(identifier, schema, Optional.empty());
  }

  private Table createTableHelper(
      TableIdentifier identifier, Schema schema, Optional<PartitionSpec> partitionSpecOpt) {
    String tableLocation = tableLocation(identifier);
    Engine engine = DefaultEngine.create(conf);
    io.delta.kernel.Table kernelTable = io.delta.kernel.Table.forPath(engine, tableLocation);
    io.delta.kernel.TransactionBuilder txnBuilder =
        kernelTable
            .createTransactionBuilder(engine, "iceberg", Operation.CREATE_TABLE)
            .withSchema(engine, SchemaUtils.fromIcebergSchema(schema.asStruct()));

    if (partitionSpecOpt.isPresent()) {
      final List<String> partSpecNames =
          partitionSpecOpt.get().fields().stream()
              .map(PartitionField::name)
              .collect(Collectors.toList());
      txnBuilder = txnBuilder.withPartitionColumns(engine, partSpecNames);

      LOG.info("Scott > createTableHelper :: partSpecNames {}", partSpecNames);
    }

    txnBuilder.build(engine).commit(engine, CloseableIterable.emptyIterable());

    LOG.info("createTable ::: Created table at path {}", tableLocation);

    return new DeltaTable(identifier, conf, tableLocation);
  }

  @Override
  public TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
    LOG.info("Scott > DeltaCatalog :: buildTable() :: identifier {}", identifier);
    return new DeltaTableBuilder(identifier, schema, conf);
  }

  private String tableLocation(TableIdentifier ident) {
    return SLASH.join(warehouse, ident.name());
  }

  @Override
  public Table loadTable(TableIdentifier ident) {
    String tableLocation = tableLocation(ident);

    LOG.info("loadTable ::: ident {}, location {}", ident, tableLocation);
    try {
      return new DeltaTable(ident, conf, tableLocation);
    } catch (io.delta.kernel.exceptions.TableNotFoundException ex) {
      LOG.info("loadTable :: TABLE NOT FOUND");
      throw new NoSuchTableException("Table not found: " + ident, ex);
    }
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    LOG.info("Initializing DeltaCatalog with name: {}", name);

    if (null == conf) {
      LOG.warn("No Hadoop Configuration was set, using the default environment Configuration");
      conf = new Configuration();
    }

    this.name = name;
    this.catalogProperties = ImmutableMap.copyOf(properties);

    if (properties.containsKey(CatalogProperties.WAREHOUSE_LOCATION)) {
      this.warehouse =
          LocationUtil.stripTrailingSlash(properties.get(CatalogProperties.WAREHOUSE_LOCATION));
      conf.set(HIVE_WAREHOUSE_PROP, warehouse); // keep the Configuration in sync
    } else {
      this.warehouse = LocationUtil.stripTrailingSlash(conf.get(HIVE_WAREHOUSE_PROP, null));
    }

    Preconditions.checkArgument(
        warehouse != null, "Missing required property: %s", CatalogProperties.WAREHOUSE_LOCATION);

    String ioImpl = properties.get(CatalogProperties.FILE_IO_IMPL);
    this.fileIO =
        ioImpl == null ? new HadoopFileIO(conf) : CatalogUtil.loadFileIO(ioImpl, properties, conf);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }
}
