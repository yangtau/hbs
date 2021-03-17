package org.yangtau.hbs;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.concurrent.CompletableFuture;

// a successfully committed txn must write a row in this table
// this is a flag so that other txns knew whether a txn is committed or aborted.
public class CommitTable {
    public static final String TABLE_NAME = "@COMMIT_TABLE";
    public static final byte[] COLUMN = Bytes.toBytes("@COMMIT");
    private static final byte[] VALUE = Bytes.toBytes("");

    private final Storage storage;

    public CommitTable(Storage storage) {
        this.storage = storage;
    }

    private KeyValue.Key timestampToKey(long timestamp) {
        return new KeyValue.Key(TABLE_NAME, Bytes.toBytes(timestamp), COLUMN);
    }

    public CompletableFuture<Boolean> exists(long timestamp) {
        return storage.exists(timestampToKey(timestamp));
    }

    public CompletableFuture<Void> commit(long timestamp) {
        return storage.put(timestampToKey(timestamp), VALUE);
    }

    public CompletableFuture<Void> drop(long timestamp) {
        return storage.remove(timestampToKey(timestamp));
    }
}