package io.delta;

import io.delta.kernel.Table;
import io.delta.kernel.engine.Engine;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeltaRewriteFiles implements RewriteFiles {

  private static final Logger LOG = LoggerFactory.getLogger(DeltaRewriteFiles.class);
  private final DeltaTable icebergDeltaTable;
  private final Table kernelDeltaTable;
  private final Engine kernelEngine;

  public DeltaRewriteFiles(DeltaTable icebergDeltaTable, Table kernelDeltaTable,
      Engine kernelEngine) {
    this.icebergDeltaTable = icebergDeltaTable;
    this.kernelDeltaTable = kernelDeltaTable;
    this.kernelEngine = kernelEngine;
  }

  @Override
  public RewriteFiles deleteFile(DataFile dataFile) {
    LOG.info("Scott > DeltaRewriteFiles :: deleteFile() :: dataFile {}", dataFile);
    return this;
  }

  @Override
  public RewriteFiles deleteFile(DeleteFile deleteFile) {
    LOG.info("Scott > DeltaRewriteFiles :: deleteFile() :: deleteFile {}", deleteFile);
    return this;
  }

  @Override
  public RewriteFiles addFile(DataFile dataFile) {
    LOG.info("Scott > DeltaRewriteFiles :: addFile() :: dataFile {}", dataFile);
    return RewriteFiles.super.addFile(dataFile);
  }

  @Override
  public RewriteFiles addFile(DeleteFile deleteFile) {
    LOG.info("Scott > DeltaRewriteFiles :: addFile() :: deleteFile {}", deleteFile);
    return this;
  }

  @Override
  public RewriteFiles addFile(DeleteFile deleteFile, long dataSequenceNumber) {
    LOG.info("Scott > DeltaRewriteFiles :: addFile() :: deleteFile {} dataSequenceNumber {}",
        deleteFile, dataSequenceNumber);
    return this;
  }

  @Override
  @Deprecated
  public RewriteFiles rewriteFiles(Set<DataFile> filesToDelete, Set<DataFile> filesToAdd,
      long sequenceNumber) {
    LOG.info("Scott > DeltaRewriteFiles :: DEPRECATED rewriteFiles AAA");
    return this;
  }

  @Override
  @Deprecated
  public RewriteFiles rewriteFiles(Set<DataFile> dataFilesToReplace,
      Set<DeleteFile> deleteFilesToReplace, Set<DataFile> dataFilesToAdd,
      Set<DeleteFile> deleteFilesToAdd) {
    LOG.info("Scott > DeltaRewriteFiles :: DEPRECATED rewriteFiles BBB");
    return this;
  }

  @Override
  public RewriteFiles validateFromSnapshot(long snapshotId) {
    LOG.info("Scott > DeltaRewriteFiles :: validateFromSnapshot() :: snapshotId {}", snapshotId);
    return this;
  }

  @Override
  public RewriteFiles set(String property, String value) {
    LOG.info("Scott > DeltaRewriteFiles :: set() :: property {} value {}", property, value);
    return this;
  }

  @Override
  public RewriteFiles deleteWith(Consumer<String> deleteFunc) {
    LOG.info("Scott > DeltaRewriteFiles :: deleteWith() :: deleteFunc {}", deleteFunc);
    return this;
  }

  @Override
  public RewriteFiles stageOnly() {
    LOG.info("Scott > DeltaRewriteFiles :: stageOnly");
    return this;
  }

  @Override
  public RewriteFiles scanManifestsWith(ExecutorService executorService) {
    LOG.info("Scott > DeltaRewriteFiles :: scanManifestsWith() :: executorService {}",
        executorService);
    return null;
  }

  @Override
  public Snapshot apply() {
    LOG.info("Scott > DeltaRewriteFiles :: apply");
    return null;
  }

  @Override
  public void commit() {
    LOG.info("Scott > DeltaRewriteFiles :: commit");
  }
}
