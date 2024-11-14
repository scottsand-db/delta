package io.delta;

import com.esotericsoftware.minlog.Log;
import io.delta.kernel.Operation;
import io.delta.kernel.Table;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.actions.RemoveFile;
import io.delta.kernel.internal.actions.SingleAction;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.utils.CloseableIterator;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import org.apache.hadoop.shaded.com.google.common.collect.Iterables;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.EnvironmentContext;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeltaDeleteFiles implements DeleteFiles {

  private static final Logger LOG = LoggerFactory.getLogger(DeltaDeleteFiles.class);
  private final DeltaTable icebergDeltaTable;
  private final Table kernelDeltaTable;
  private final Engine kernelEngine;
  private final Set<String> deletePaths = new HashSet<>();
  private final Set<Row> removeFileSingleActionRows = new HashSet<>();
  private final Map<String, String> parameters = Maps.newHashMap();

  public DeltaDeleteFiles(DeltaTable icebergDeltaTable, Table kernelDeltaTable,
      Engine kernelEngine) {
    this.icebergDeltaTable = icebergDeltaTable;
    this.kernelDeltaTable = kernelDeltaTable;
    this.kernelEngine = kernelEngine;
    parameters.put("iceberg-operation", DataOperations.DELETE);
  }

  @Override
  public DeleteFiles deleteFile(CharSequence path) {
    LOG.info("Scott > deleteFIle :: path {}", path);
    deletePaths.add(path.toString());
    return this;
  }

  @Override
  public DeleteFiles deleteFromRowFilter(Expression expr) {
    LOG.info("Scott > deleteFromRowFilter :: expr {}", expr);
    Expression boundExpr = Binder.bind(icebergDeltaTable.schema().asStruct(), expr,
        false /* case sensitive*/);
    LOG.info("Scott > deleteFromRowFilter :: boundExpr ??? {}", boundExpr);
    final io.delta.kernel.expressions.Predicate kernelPredicate = DeltaExpressionUtil.convert(
        boundExpr);
    LOG.info("Scott > deleteFromRowFilter :: kernelPredicate {}", kernelPredicate);
    final io.delta.kernel.Scan kernelScan = kernelDeltaTable.getLatestSnapshot(kernelEngine)
        .getScanBuilder(kernelEngine).withFilter(kernelEngine, kernelPredicate).build();
    final CloseableIterator<FilteredColumnarBatch> batchIter = kernelScan.getScanFiles(
        kernelEngine);
    batchIter.forEachRemaining(batch -> {
      final CloseableIterator<Row> batchRows = batch.getRows();
      batchRows.forEachRemaining(row -> {
        final Row addFileRow = InternalScanFileUtils.getAddFileEntry(row);
        final String addFilePath = addFileRow.getString(InternalScanFileUtils.ADD_FILE_PATH_ORDINAL);
        final long addFileSize = addFileRow.getLong(InternalScanFileUtils.ADD_FILE_SIZE_ORDINAL);
        LOG.info("Scott > deleteFromRowFilter :: row {}", row);
        final io.delta.kernel.data.MapValue partitionValues = row.getStruct(
                InternalScanFileUtils.ADD_FILE_ORDINAL)
            .getMap(InternalScanFileUtils.ADD_FILE_PARTITION_VALUES_ORDINAL);
        final Map<Integer, Object> removeFileOrdinalMap = new HashMap<>();
        removeFileOrdinalMap.put(0, addFilePath);
        removeFileOrdinalMap.put(1, System.currentTimeMillis() /* deletionTimestamp */);
        removeFileOrdinalMap.put(2, true /* isDataChange */);
        removeFileOrdinalMap.put(3, false /* extendedFileMetadata */);
        removeFileOrdinalMap.put(4, partitionValues);
        removeFileOrdinalMap.put(5, addFileSize);
        // stats = null
        // tags = null
        // dv = null
        GenericRow removeFileRow = new GenericRow(RemoveFile.FULL_SCHEMA, removeFileOrdinalMap);
        String removeFileRowStr = JsonUtils.rowToJson(removeFileRow);
        Log.info("Scott > deleteFromRowFilter :: removeFileRow {}", removeFileRowStr);
        removeFileSingleActionRows.add(SingleAction.createRemoveFileSingleAction(removeFileRow));
      });
    });

    return this;
  }

  @Override
  public DeleteFiles caseSensitive(boolean caseSensitive) {
    LOG.info("Scott > deleteFromRowFilter :: caseSensitive {}", caseSensitive);
    return this;
  }

  @Override
  public DeleteFiles set(String property, String value) {
    LOG.info("Scott > set :: property {} value {}", property, value);
    return this;
  }

  @Override
  public DeleteFiles deleteWith(Consumer<String> deleteFunc) {
    LOG.info("Scott > deleteWith :: deleteFunc {}", deleteFunc);
    throw new UnsupportedOperationException("deleteWith not implemented yet");
  }

  @Override
  public DeleteFiles stageOnly() {
    LOG.info("Scott > stageOnly");
    return this;
  }

  @Override
  public DeleteFiles scanManifestsWith(ExecutorService executorService) {
    LOG.info("Scott > scanManifestsWith :: executorService {}", executorService);
    return this;
  }

  @Override
  public Snapshot apply() {
    LOG.info("Scott > apply");
    return null;
  }

  @Override
  public void commit() {
    LOG.info("Scott > commit");
    icebergDeltaTable.refresh();

    removeFileSingleActionRows.forEach(row -> LOG.info("RemoveRow: {}", JsonUtils.rowToJson(row)));

    long now = System.currentTimeMillis();

    TransactionCommitResult result = kernelDeltaTable
      .createTransactionBuilder(kernelEngine, engineInfo(), Operation.DELETE)
      .build(kernelEngine)
      .commit(kernelEngine, InMemoryCloseableIterable.of(removeFileSingleActionRows));

    LOG.info("Scott > COMMIT SUCCESS > result {}", result);

    icebergDeltaTable.refresh();
  }

  private static String engineInfo() {
    Map<String, String> env = EnvironmentContext.get();
    String engine = env.get(EnvironmentContext.ENGINE_NAME);
    String version = env.get(EnvironmentContext.ENGINE_VERSION);
    String icebergVersion = IcebergBuild.version();
    return String.format("%s/%s Iceberg/%s", engine, version, icebergVersion);
  }
}
