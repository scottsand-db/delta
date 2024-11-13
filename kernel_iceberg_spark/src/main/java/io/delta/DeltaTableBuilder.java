package io.delta;

import io.delta.kernel.Operation;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.CloseableIterable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionField;
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
  private final Configuration conf;

  private PartitionSpec partitionSpec = null;
  private String location = null;

  public DeltaTableBuilder(TableIdentifier identifier, Schema schema, Configuration conf) {
    this.identifier = identifier;
    this.schema = schema;
    this.conf = conf;
  }

  @Override
  public TableBuilder withPartitionSpec(PartitionSpec spec) {
    LOG.info("Scott > DeltaTableBuilder > withPartitionSpec :: {}", spec);
    this.partitionSpec = spec;
    return this;
  }

  @Override
  public TableBuilder withSortOrder(SortOrder sortOrder) {
    return null;
  }

  @Override
  public TableBuilder withLocation(String location) {
    LOG.info("Scott > DeltaTableBuilder > withLocation :: {}", location);
    this.location = location;
    return this;
  }

  @Override
  public TableBuilder withProperties(Map<String, String> properties) {
    LOG.info("Scott > DeltaTableBuilder > withProperties :: {}", properties);
    return this;
  }

  @Override
  public TableBuilder withProperty(String key, String value) {
    LOG.info("Scott > DeltaTableBuilder > withProperty :: {}->{}", key, value);
    return this;
  }

  @Override
  public Table create() {
    LOG.info("Scott > DeltaTableBuilder > create");
    Engine engine = DefaultEngine.create(conf);
    io.delta.kernel.Table kernelTable = io.delta.kernel.Table.forPath(engine, location);
    io.delta.kernel.TransactionBuilder txnBuilder =
        kernelTable
            .createTransactionBuilder(engine, "iceberg", Operation.CREATE_TABLE)
            .withSchema(engine, SchemaUtils.fromIcebergSchema(schema.asStruct()));

    if (partitionSpec != null) {
      final List<String> partSpecNames =
          partitionSpec.fields().stream().map(PartitionField::name).collect(Collectors.toList());
      txnBuilder = txnBuilder.withPartitionColumns(engine, partSpecNames);

      LOG.info("Scott > DeltaTableBuilder > create :: partSpecNames {}", partSpecNames);
    }

    txnBuilder.build(engine).commit(engine, CloseableIterable.emptyIterable());

    LOG.info("DeltaTableBuilder::create :::: Created table: {} with schema {}", identifier, schema);
    return new DeltaTable(identifier, conf, location);
  }

  @Override
  public Transaction createTransaction() {
    LOG.info("Scott > DeltaTableBuilder > createTransaction");
    return null;
  }

  @Override
  public Transaction replaceTransaction() {
    LOG.info("Scott > DeltaTableBuilder > replaceTransaction");
    return null;
  }

  @Override
  public Transaction createOrReplaceTransaction() {
    LOG.info("Scott > DeltaTableBuilder > createOrReplaceTransaction");
    return null;
  }
}
