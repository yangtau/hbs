package org.yangtau.hbs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HBSTransaction implements Transaction {
    private final long timestamp;
    private final MVCCStorage storage;
    private final TransactionManager manager;
    private final CommitTable commitTable;

    private final Map<KeyValue.Key, byte[]> writeSet;
    private final Map<KeyValue.Key, KeyValue.Value> readSet;
    private Status status;

    public HBSTransaction(MVCCStorage mvccStorage, TransactionManager manager, CommitTable commitTable)
            throws Exception {
        this.timestamp = manager.allocate();
        this.storage = mvccStorage;
        this.manager = manager;
        this.commitTable = commitTable;

        writeSet = new HashMap<>();
        readSet = new HashMap<>();
        status = Status.Uncommitted;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    // wait for txn(timestamp) exiting, and clean commit flag in (table, row, column, timestamp) if txn committed,
    // otherwise clean this data cell
    // return true if the txn committed
    private boolean waitAndClean(KeyValue.Key key, long timestamp) throws Exception {
        manager.waitIfExists(timestamp);
        if (commitTable.exists(timestamp).join()) {
            // do not join, because the flag is not crucial to the correctness
            storage.cleanUncommittedFlag(key, timestamp);
            return true;
        } else {
            storage.removeCell(key, timestamp).join();
            return false;
        }
    }

    @Override
    public byte[] get(String table, byte[] row, byte[] col) throws Exception {
        var key = new KeyValue.Key(table, row, col);

        // try to read in writeSet
        var value = writeSet.get(key);
        if (value != null) return value;

        // no such key in the writeSet, try to read in readSet
        var v = readSet.get(key);
        if (v != null) return v.value();

        // try to read in storage
        while (true) {
            var res = storage.getWithReadTimestamp(key, timestamp).join();
            if (res == null) return null;
            if (!res.committed()) {
                if (waitAndClean(key, res.timestamp())) {
                    readSet.put(key, res);
                    return res.value();
                }
                // else: try to read an older version
            }
        }
    }

    @Override
    public void put(String table, byte[] row, byte[] col, byte[] value) throws Exception {
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
            if (!storage.putIfNoConflict(key, value, timestamp).join()) {
                abortAndClean(writtenList);
                return false;
            }
            writtenList.add(key);
        }

        // - COMMIT POINT:
        commitTable.commit(timestamp).join();
        status = Status.Committed;

        // - SECOND PHASE: clean uncommitted flags
        storage.cleanUncommittedFlags(writeSet.keySet(), timestamp).join();
        return true;
    }

    private void abortAndClean(List<KeyValue.Key> writtenList) throws Exception {
        status = Status.Aborted;
        manager.release(timestamp);

        // clean data that have already been written
        storage.removeCells(writtenList, timestamp).join();
    }

    @Override
    public void abort() throws Exception {
        expectUncommitted();
        abortAndClean(List.of());
    }

    private void expectUncommitted() throws Exception {
        if (status != Status.Uncommitted)
            throw new Exception("The transaction is not uncommitted, status: " + status.toString());
    }

    // possible changes:
    // Uncommitted -> Committed
    // Uncommitted -> Aborted
    enum Status {
        Committed,
        Aborted,
        Uncommitted,
    }
}
