package io.delta;

import io.delta.kernel.Operation;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.CloseableIterable;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog.TableBuilder;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeltaTableBuilder implements TableBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(DeltaTableBuilder.class);

  private final TableIdentifier identifier;
  private final Schema schema;
  private String location;
  private final Configuration conf;

  public DeltaTableBuilder(
      TableIdentifier identifier, Schema schema, String location, Configuration conf) {
    this.identifier = identifier;
    this.schema = schema;
    this.location = location;
    this.conf = conf;
  }

  @Override
  public TableBuilder withPartitionSpec(PartitionSpec spec) {
    return null;
  }

  @Override
  public TableBuilder withSortOrder(SortOrder sortOrder) {
    return null;
  }

  @Override
  public TableBuilder withLocation(String location) {
    this.location = location;
    return this;
  }

  @Override
  public TableBuilder withProperties(Map<String, String> properties) {
    return null;
  }

  @Override
  public TableBuilder withProperty(String key, String value) {
    return null;
  }

  @Override
  public Table create() {
    Engine engine = DefaultEngine.create(conf);
    io.delta.kernel.Table kernelTable = io.delta.kernel.Table.forPath(engine, location);
    kernelTable
        .createTransactionBuilder(engine, "iceberg", Operation.CREATE_TABLE)
        .withSchema(engine, SchemaUtils.fromIcebergSchema(schema.asStruct()))
        .build(engine)
        .commit(engine, CloseableIterable.emptyIterable());

    LOG.info("DeltaTableBuilder::create :::: Created table: {} with schema {}", identifier, schema);
    return new DeltaTable(identifier, conf, location);
  }

  @Override
  public Transaction createTransaction() {
    return null;
  }

  @Override
  public Transaction replaceTransaction() {
    return null;
  }

  @Override
  public Transaction createOrReplaceTransaction() {
    return null;
  }
}
