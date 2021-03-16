package org.yangtau.hbs;

import org.apache.hadoop.hbase.util.Bytes;

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

    public boolean exists(long timestamp) throws Exception {
        return storage.exists(TABLE_NAME, Bytes.toBytes(timestamp), COLUMN).get();
    }

    public void commit(long timestamp) throws Exception {
        storage.put(TABLE_NAME, Bytes.toBytes(timestamp), COLUMN, VALUE).get();
    }

    public void delete(long timestamp) throws Exception {
        storage.remove(TABLE_NAME, Bytes.toBytes(timestamp), COLUMN).get();
    }
}