package org.yangtau.hbs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// TODO(yangtau): check if exception may cause inconsistency

public class HBSTransaction implements Transaction {
    private final long timestamp;
    private final MVCCStorage storage;
    private final TransactionManager manager;
    private final CommitTable commitTable;

    private final Map<KeyValue.Key, byte[]> writeSet;
    private final Map<KeyValue.Key, KeyValue.Value> readSet;

    public HBSTransaction(MVCCStorage mvccStorage, TransactionManager manager, CommitTable commitTable)
            throws Exception {
        this.timestamp = manager.allocate();
        this.storage = mvccStorage;
        this.manager = manager;
        this.commitTable = commitTable;

        writeSet = new HashMap<>();
        readSet = new HashMap<>();
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
        // txn is lost connection with TM (committed, or the txn crashes)
        while (true) {
            Status s = commitTable.status(timestamp).join();
            if (s == Status.Uncommitted) {
                // try to abort txn(timestamp), because it lost the connection with TM
                if (commitTable.abort(timestamp).join())
                    s = Status.Aborted;
                else // fail to abort the txn (there may be concurrent status modification),  try to read again.
                    continue;
            }

            if (s == Status.Committed) {
                // try to clean the uncommitted flag in the data cell
                storage.cleanUncommittedFlag(key, timestamp).join();
                return true;
            } else if (s == Status.Aborted) {
                // remove the aborted data cell
                storage.removeCell(key, timestamp).join();
                return false;
            }
        }
    }

    @Override
    public byte[] get(String table, byte[] row, byte[] col) throws Exception {
        KeyValue.Key key = new KeyValue.Key(table, row, col);

        // try to read in writeSet
        byte[] value = writeSet.get(key);
        if (value != null) return value;

        // no such key in the writeSet, try to read in readSet
        KeyValue.Value v = readSet.get(key);
        if (v != null) return v.value();

        // try to read in storage
        while (true) {
            KeyValue.Value res = storage.getWithReadTimestamp(key, timestamp).join();
            if (res == null) return null;
            if (!res.committed()) {
                if (waitAndClean(key, res.timestamp())) {
                    readSet.put(key, res);
                    return res.value();
                }
                // else: try to read an older version
            } else {
                readSet.put(key, res);
                return res.value();
            }
        }
    }

    @Override
    public void put(String table, byte[] row, byte[] col, byte[] value) throws Exception {
        writeSet.put(new KeyValue.Key(table, row, col), value);
    }

    private void failCommit(List<KeyValue.Key> cleanList, boolean writeCommitTable) throws Exception {
        storage.removeCells(cleanList, timestamp).join();
        if (!cleanList.isEmpty() && writeCommitTable) commitTable.abort(timestamp).get();
        manager.release(timestamp);
    }

    @Override
    public boolean commit() throws Exception {
        // optimize for READ ONLY
        if (writeSet.isEmpty()) {
            // no need for 2PC
            manager.release(timestamp);
            return true;
        }

        // TODO: concurrent prewrite and clean

        // - FIRST PHASE: write data if no conflict
        List<KeyValue.Key> writtenList = new ArrayList<>();
        for (Map.Entry<KeyValue.Key, byte[]> e : writeSet.entrySet()) {
            KeyValue.Key key = e.getKey();
            byte[] value = e.getValue();
            if (!storage.putIfNoConflict(key, value, timestamp).join()) {
                failCommit(writtenList, true);
                return false;
            }
            writtenList.add(key);
        }

        // - COMMIT POINT:
        if (!commitTable.commit(timestamp).join()) {
            failCommit(writtenList, false);
            return false;
        }

        try {
            manager.release(timestamp);

            // - SECOND PHASE: clean uncommitted flags
            storage.cleanUncommittedFlags(writeSet.keySet(), timestamp).join();
        } catch (Exception e) {
            // ignore all exception after successful commitment
        }
       return true;
    }


    @Override
    public void abort() throws Exception {
        manager.release(timestamp);
        writeSet.clear();
    }
}
