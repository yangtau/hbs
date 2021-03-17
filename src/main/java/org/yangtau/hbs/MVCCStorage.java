package org.yangtau.hbs;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface MVCCStorage extends Storage {
    // create a table which has multi-version for each cell
    CompletableFuture<Void> createMVCCTable(String table, List<byte[]> cols);

    // GET API used by MVTO algorithm:
    // get the newest version in (table, row, column) before `readTimestamp` and update the greatest RT for this version
    // return null if failed to get anything (error, or there is nothing)
    CompletableFuture<KeyValue.Value> getWithReadTimestamp(KeyValue.Key key, long readTimestamp);

    // PUT API used by MVTO algorithm:
    // put if the newest version in (table, row, column) before `writeTimestamp` has less RT than writeTimestamp,
    // thus, there is no conflict between this write and reads before this write
    // return false if failed (error, or conflict)
    CompletableFuture<Boolean> putIfNoConflict(KeyValue.Key key, byte[] value, long writeTimestamp);

    // remove cell of `key, timestamp`
    CompletableFuture<Void> removeCell(KeyValue.Key key, long timestamp);

    // remove cells with `timestamp` in `keys`
    CompletableFuture<Void> removeCells(Collection<KeyValue.Key> keys, long timestamp);

    // clean uncommitted flag of `key, timestamp`
    CompletableFuture<Void> cleanUncommittedFlag(KeyValue.Key key, long timestamp);

    // clean uncommitted flag of cells in `keys`
    CompletableFuture<Void> cleanUncommittedFlags(Collection<KeyValue.Key> keys, long timestamp);
}
