package org.yangtau.hbs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class HBSTransaction implements Transaction {
    private final long timestamp;
    private final Storage storage;
    private final TransactionManager manager;
    private final CommitTable commitTable;

    private final Map<KeyValue.Key, byte[]> writeSet;
    private Status status;

    public HBSTransaction(Storage storage, TransactionManager manager) throws Exception {
        this.timestamp = manager.allocate();
        this.storage = storage;
        this.manager = manager;
        this.commitTable = new CommitTable(storage);

        writeSet = new HashMap<>();
        status = Status.Uncommitted;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    // wait for txn(timestamp) exiting, and clean commit flag in (table, row, col, timestamp) if txn committed,
    // otherwise clean this data cell
    // return true if the txn committed
    private boolean waitAndClean(KeyValue.Key key, long timestamp) throws Exception {
        manager.waitIfExists(timestamp);
        if (commitTable.exists(timestamp)) {
            // TODO: try to clean commitFlag
            return true;
        } else {
            // TODO: will this fail?
            storage.cleanCell(key, timestamp).get();
            return false;
        }
    }

    @Override
    public byte[] get(String table, byte[] row, byte[] col) throws Exception {
        expectUncommitted();
        var key = new KeyValue.Key(table, row, col);

        // try to read in writeSet
        var value = writeSet.get(key);
        if (value != null) return value;

        // no such key in writeSet
        // TODO: cache read set
        while (true) {
            var res = storage.getWithReadTimestamp(key, timestamp).get();
            if (res == null) return null;
            if (!res.committed()) {
                if (waitAndClean(key, timestamp))
                    return res.value();
                // else: try to read an older version
            }
        }
    }

    @Override
    public void put(String table, byte[] row, byte[] col, byte[] value) throws Exception {
        expectUncommitted();
        writeSet.put(new KeyValue.Key(table, row, col), value);
    }

    @Override
    public boolean commit() throws Exception {
        expectUncommitted();

        // - FIRST PHASE: write data if no conflict
        var writtenList = new ArrayList<KeyValue.Key>();
        for (var e : writeSet.entrySet()) {
            var key = e.getKey();
            var value = e.getValue();
            if (!storage.putIfNoConflict(key, value, timestamp).get()) {
                manager.release(timestamp);
                // clean data that have already been written
                storage.cleanCells(writtenList, timestamp).get();
                return false;
            }
            writtenList.add(key);
        }

        // - COMMIT POINT:
        commitTable.commit(timestamp);
        status = Status.Committed;

        // - SECOND PHASE: clean uncommitted flags
        var keyList = writeSet.keySet();
        storage.cleanUncommittedFlag(keyList, timestamp).get();
        return true;
    }

    @Override
    public void abort() throws Exception {
        expectUncommitted();
        status = Status.Aborted;
        manager.release(timestamp);
        // ignore
    }

    private void expectUncommitted() throws Exception {
        if (status != Status.Uncommitted)
            throw new Exception("The transaction is not uncommitted, status: " + status.toString());
    }

    enum Status {
        Committed,
        Aborted,
        Uncommitted,
    }
}
