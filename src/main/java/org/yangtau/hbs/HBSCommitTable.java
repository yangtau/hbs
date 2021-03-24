package org.yangtau.hbs;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Strings;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

// a successfully committed txn must write a row in this table
// this is a flag so that other txns knew whether a txn is committed or aborted.
public class HBSCommitTable implements CommitTable {
    public static final String TABLE_NAME = "HBS_COMMIT_TABLE";
    public static final byte[] COLUMN = Bytes.toBytes("@COMMIT");
    public static final byte[] COMMITTED = Bytes.toBytes("Y");
    public static final byte[] UNCOMMITTED = Bytes.toBytes("N");
    private final Storage storage;

    public HBSCommitTable(Storage storage) {
        this.storage = storage;
    }

    private KeyValue.Key timestampToKey(long timestamp) {
        return new KeyValue.Key(TABLE_NAME, Bytes.toBytes(String.valueOf(timestamp)), COLUMN);
    }

    public CompletableFuture<Transaction.Status> status(long timestamp) {
        return storage.get(timestampToKey(timestamp))
                .thenApplyAsync(
                        (v) -> {
                            if (v == null) return Transaction.Status.Uncommitted;
                            if (Arrays.equals(v, COMMITTED)) return Transaction.Status.Committed;
                            return Transaction.Status.Aborted;
                        }
                );
    }

    public CompletableFuture<Boolean> commit(long timestamp) {
        return storage.putIfNotExists(timestampToKey(timestamp), COMMITTED);
    }

    public CompletableFuture<Boolean> abort(long timestamp) {
        return storage.putIfNotExists(timestampToKey(timestamp), UNCOMMITTED);
    }
}