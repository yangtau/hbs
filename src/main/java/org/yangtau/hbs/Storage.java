package org.yangtau.hbs;

import java.util.List;
import java.util.concurrent.CompletableFuture;

// Interface for MVCC Storage
public interface Storage {
    CompletableFuture<Void> createTable(String table, List<byte[]> cols, boolean multiVersion);

    CompletableFuture<Void> removeTable(String table);

    // put `(row, col, timestamp) => value` in `table`
    // timestamp is a timestamp assigned by storage which should be greater than all existed timestamps
    CompletableFuture<Void> put(String table, byte[] row, byte[] col, byte[] value);

    // get value of `(row, col)` in `table` with the greatest timestamp
    CompletableFuture<byte[]> get(String table, byte[] row, byte[] col);

    // check if `(row, col)` exists with any timestamp
    CompletableFuture<Boolean> exists(String table, byte[] row, byte[] col);

    // delete all `(row, col)`
    CompletableFuture<Void> remove(String table, byte[] row, byte[] col);

    // GET API used by MVTO algorithm:
    // get the newest version in (table, row, col) before `readTimestamp` and update the greatest RT for this version
    // return null if failed to get anything (error, or there is nothing)
    CompletableFuture<Cell> getWithReadTimestamp(String table, byte[] row, byte[] col, long readTimestamp);

    // PUT API used by MVTO algorithm:
    // put if the newest version in (table, row, col) before `writeTimestamp` has less RT than writeTimestamp,
    // thus, there is no conflict between this write and reads before this write
    // return false if failed (error, or conflict)
    CompletableFuture<Boolean> putIfNoConflict(String table, byte[] row, byte[] col, byte[] value, long writeTimestamp);

    record Cell(byte[] value, boolean committed, long timestamp){}
}
