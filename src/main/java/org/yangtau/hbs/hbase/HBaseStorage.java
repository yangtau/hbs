package org.yangtau.hbs.hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.yangtau.hbs.KeyValue;
import org.yangtau.hbs.MVCCStorage;
import org.yangtau.hbs.hbase.coprocessor.GetEndpoint;
import org.yangtau.hbs.hbase.coprocessor.PutEndpoint;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

// Implementation of Storage in HBase
public class HBaseStorage implements MVCCStorage {
    private final AsyncConnection connection;
    private final ConcurrentHashMap<String, AsyncTable<AdvancedScanResultConsumer>> tableMap;

    public HBaseStorage(AsyncConnection conn) {
        connection = conn;
        tableMap = new ConcurrentHashMap<>();
    }

    private AsyncTable<AdvancedScanResultConsumer> getTable(String tableName) {
        if (tableMap.containsKey(tableName)) {
            return tableMap.get(tableName);
        } else {
            var table = connection.getTable(TableName.valueOf(tableName));
            tableMap.put(tableName, table);
            return table;
        }
    }

    private CompletableFuture<Void> createTable(String table, List<byte[]> cols,
                                                Function<byte[], ColumnFamilyDescriptor> columnDescriptorCreator) {
        var builder = TableDescriptorBuilder.newBuilder(TableName.valueOf(table));
        cols.forEach((col) -> builder.setColumnFamily(columnDescriptorCreator.apply(col)));
        return connection.getAdmin().createTable(builder.build());
    }


    @Override
    public CompletableFuture<Void> createTable(String table, List<byte[]> cols) {
        return createTable(table, cols,
                (col) -> ColumnFamilyDescriptorBuilder.newBuilder(col).build());
    }

    @Override
    public CompletableFuture<Void> removeTable(String table) {
        var tableName = TableName.valueOf(table);
        var admin = connection.getAdmin();
        return admin.disableTable(tableName)
                .thenComposeAsync(v -> admin.deleteTable(tableName));
    }

    private CompletableFuture<Void> put(String table, Put put) {
        return getTable(table).put(put);
    }

    private CompletableFuture<Result> get(String table, Get get) {
        return getTable(table).get(get);
    }

    private CompletableFuture<Boolean> exists(String table, Get get) {
        get.setCheckExistenceOnly(true);
        return getTable(table).exists(get);
    }

    private CompletableFuture<Void> remove(String table, Delete delete) {
        return getTable(table).delete(delete);
    }

    private CompletableFuture<Void> removeAll(String table, List<Delete> delete) {
        return getTable(table).deleteAll(delete);
    }

    private CompletableFuture<Boolean> checkAndMutate(String table, CheckAndMutate checkAndMutate) {
        return getTable(table).checkAndMutate(checkAndMutate)
                .thenApplyAsync(CheckAndMutateResult::isSuccess);
    }

    // remove cells in multi tables, multi rows
    private CompletableFuture<Void> removeAllInMultiTables(Collection<KeyValue.Key> keys,
                                                        Function<KeyValue.Key, Delete> deleteCreator) {
        var futures = keys.stream()
                .collect(Collectors.groupingBy(KeyValue.Key::table, // group by table name
                        Collectors.mapping(deleteCreator, Collectors.toList()))) // table -> List<Delete>
                .entrySet()
                .stream()
                .map(e -> removeAll(e.getKey(), e.getValue()))
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(futures);

    }

    @Override
    public CompletableFuture<Void> put(KeyValue.Key key, byte[] value) {
        var put = new Put(key.row())
                .addColumn(key.column(), Constants.DATA_QUALIFIER_BYTES, value);
        return put(key.table(), put);
    }

    @Override
    public CompletableFuture<byte[]> get(KeyValue.Key key) {
        var get = new Get(key.row())
                .addColumn(key.column(), Constants.DATA_QUALIFIER_BYTES);
        return get(key.table(), get)
                .thenApplyAsync((r) -> r.getValue(key.column(), Constants.DATA_QUALIFIER_BYTES));
    }

    @Override
    public CompletableFuture<Boolean> exists(KeyValue.Key key) {
        var get = new Get(key.row())
                .addColumn(key.column(), Constants.DATA_QUALIFIER_BYTES);
        return exists(key.table(), get);
    }

    @Override
    public CompletableFuture<Void> remove(KeyValue.Key key) {
        var delete = new Delete(key.row()).addColumn(key.column(), Constants.DATA_QUALIFIER_BYTES);
        return remove(key.table(), delete);
    }

    @Override
    public CompletableFuture<Boolean> putIfNotExists(KeyValue.Key key, byte[] value) {
        var put = new Put(key.row())
                .addColumn(key.column(), Constants.DATA_QUALIFIER_BYTES, value);
        var checkAndMutate = CheckAndMutate.newBuilder(key.row())
                .ifNotExists(key.column(), Constants.DATA_QUALIFIER_BYTES)
                .build(put);
        return checkAndMutate(key.table(), checkAndMutate);
    }

    @Override
    public CompletableFuture<Void> createMVCCTable(String table, List<byte[]> cols) {
        // multi-version for each cell
        return createTable(
                table, cols,
                (col) -> ColumnFamilyDescriptorBuilder.newBuilder(col)
                        .setTimeToLive(Integer.MAX_VALUE)
                        .setMaxVersions(Integer.MAX_VALUE)
                        .build()
        );
    }

    // Read data and record the newest read timestamp for each version.
    // In HBase, we use @GetEndpoint to implement this.
    @Override
    public CompletableFuture<KeyValue.Value> getWithReadTimestamp(KeyValue.Key key, long readTimestamp) {
        return GetEndpoint.runAsync(getTable(key.table()), key, readTimestamp);
    }


    @Override
    public CompletableFuture<Boolean> putIfNoConflict(KeyValue.Key key, byte[] value, long writeTimestamp) {
        return PutEndpoint.runAsync(getTable(key.table()), key, value, writeTimestamp);
    }


    @Override
    public CompletableFuture<Void> removeCell(KeyValue.Key key, long timestamp) {
        var delete = new Delete(key.row())
                .addFamilyVersion(key.column(), timestamp);
        return remove(key.table(), delete);
    }


    @Override
    public CompletableFuture<Void> removeCells(Collection<KeyValue.Key> keys, long timestamp) {
        return removeAllInMultiTables(keys,
                k -> new Delete(k.row()).addFamilyVersion(k.column(), timestamp));
    }

    @Override
    public CompletableFuture<Void> cleanUncommittedFlag(KeyValue.Key key, long timestamp) {
        var delete = new Delete(key.row())
                .addColumn(key.column(), Constants.UNCOMMITTED_QUALIFIER_BYTES, timestamp);
        return connection.getTable(TableName.valueOf(key.table()))
                .delete(delete);
    }

    @Override
    public CompletableFuture<Void> cleanUncommittedFlags(Collection<KeyValue.Key> keys, long timestamp) {
        return removeAllInMultiTables(keys,
                k -> new Delete(k.row()).addColumn(k.column(), Constants.UNCOMMITTED_QUALIFIER_BYTES, timestamp));
    }
}
