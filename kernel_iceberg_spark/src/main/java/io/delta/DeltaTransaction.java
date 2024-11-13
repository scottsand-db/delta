package io.delta;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateLocation;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;

public class DeltaTransaction implements Transaction {

  @Override
  public Table table() {
    return null;
  }

  @Override
  public UpdateSchema updateSchema() {
    return null;
  }

  @Override
  public UpdatePartitionSpec updateSpec() {
    return null;
  }

  @Override
  public UpdateProperties updateProperties() {
    return null;
  }

  @Override
  public ReplaceSortOrder replaceSortOrder() {
    return null;
  }

  @Override
  public UpdateLocation updateLocation() {
    return null;
  }

  @Override
  public AppendFiles newAppend() {
    return null;
  }

  @Override
  public RewriteFiles newRewrite() {
    return null;
  }

  @Override
  public RewriteManifests rewriteManifests() {
    return null;
  }

  @Override
  public OverwriteFiles newOverwrite() {
    return null;
  }

  @Override
  public RowDelta newRowDelta() {
    return null;
  }

  @Override
  public ReplacePartitions newReplacePartitions() {
    return null;
  }

  @Override
  public DeleteFiles newDelete() {
    return null;
  }

  @Override
  public ExpireSnapshots expireSnapshots() {
    return null;
  }

  @Override
  public void commitTransaction() {

  }
}
