package org.yangtau.hbs;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

// Interface for MVCC Storage
public interface Storage {
    CompletableFuture<Void> createTable(String table, List<byte[]> cols, boolean multiVersion);

    CompletableFuture<Void> removeTable(String table);

    // put `(row, col, timestamp) => value` in `table`
    // timestamp is a timestamp assigned by storage which should be greater than all existed timestamps
    @Deprecated
    CompletableFuture<Void> put(String table, byte[] row, byte[] col, byte[] value);

    CompletableFuture<Void> put(KeyValue.Key key, byte[] value);

    // get value of `(row, col)` in `table` with the greatest timestamp
    @Deprecated
    CompletableFuture<byte[]> get(String table, byte[] row, byte[] col);

    CompletableFuture<byte[]> get(KeyValue.Key key);

    // check if `(row, col)` exists with any timestamp
    @Deprecated
    CompletableFuture<Boolean> exists(String table, byte[] row, byte[] col);

    CompletableFuture<Boolean> exists(KeyValue.Key key);


    // delete all `(row, col)`
    @Deprecated
    CompletableFuture<Void> remove(String table, byte[] row, byte[] col);

    CompletableFuture<Void> remove(KeyValue.Key key);

    // GET API used by MVTO algorithm:
    // get the newest version in (table, row, col) before `readTimestamp` and update the greatest RT for this version
    // return null if failed to get anything (error, or there is nothing)
    @Deprecated
    CompletableFuture<KeyValue.Value> getWithReadTimestamp(String table, byte[] row, byte[] col, long readTimestamp);

    CompletableFuture<KeyValue.Value> getWithReadTimestamp(KeyValue.Key key, long readTimestamp);


    // PUT API used by MVTO algorithm:
    // put if the newest version in (table, row, col) before `writeTimestamp` has less RT than writeTimestamp,
    // thus, there is no conflict between this write and reads before this write
    // return false if failed (error, or conflict)
    @Deprecated
    CompletableFuture<Boolean> putIfNoConflict(String table, byte[] row, byte[] col, byte[] value, long writeTimestamp);

    CompletableFuture<Boolean> putIfNoConflict(KeyValue.Key key, byte[] value, long writeTimestamp);


    CompletableFuture<Void> cleanCell(KeyValue.Key key, long timestamp);

    // clean cells with `timestamp` in the list
    CompletableFuture<Void> cleanCells(Collection<KeyValue.Key> keys, long timestamp);

    // clean uncommitted flag of cells in the list
    CompletableFuture<Void> cleanUncommittedFlag(Collection<KeyValue.Key> keys, long timestamp);
}
