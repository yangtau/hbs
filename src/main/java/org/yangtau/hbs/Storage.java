package org.yangtau.hbs;

import java.util.List;
import java.util.concurrent.CompletableFuture;

// Interface for MVCC Storage
public interface Storage {
    CompletableFuture<Void> createTable(String table, List<byte[]> cols);

    CompletableFuture<Void> removeTable(String table);

    // put `(row, column, timestamp) => value` in `table`
    // timestamp is a timestamp assigned by storage which should be greater than all existed timestamps
    CompletableFuture<Void> put(KeyValue.Key key, byte[] value);

    // get value of `(row, column)` in `table` with the greatest timestamp
    CompletableFuture<byte[]> get(KeyValue.Key key);

    // check if `(row, column)` exists with any timestamp
    CompletableFuture<Boolean> exists(KeyValue.Key key);


    // delete all `(row, column)`
    CompletableFuture<Void> remove(KeyValue.Key key);
}
