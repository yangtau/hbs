import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;
import org.yangtau.hbs.*;
import org.yangtau.hbs.hbase.HBaseStorage;
import org.yangtau.hbs.zookeeper.ZKTransactionManager;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TransactionTest {

    private final AsyncConnection conn;
    private final MVCCStorage storage;
    private final Map<Long, AsyncConnection> connections;
    private final TransactionManager manager;

    public TransactionTest() {
        conn = ConnectionFactory.createAsyncConnection(HBaseConfiguration.create()).join();
        storage = new HBaseStorage(conn);
        connections = new HashMap<>();

        try {
            ZKTransactionManager.createParentNode("localhost");
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }

        try {
            storage.createTable(HBSCommitTable.TABLE_NAME, List.of(HBSCommitTable.COLUMN)).join();
        } catch (CompletionException e) {
            System.err.println(e.getMessage());
        }

        manager = new ZKTransactionManager("localhost");
    }

    Transaction createTxn() throws Exception {
        var con = ConnectionFactory
                .createAsyncConnection(HBaseConfiguration.create()).join();

        var s = new HBaseStorage(con);
        Transaction txn = new HBSTransaction(s,
                manager,
                new HBSCommitTable(s));

        connections.put(txn.getTimestamp(), con);
        return txn;
    }

    void releaseTxn(Transaction txn) throws IOException {
        assertNotEquals(txn.getStatus(), Transaction.Status.Uncommitted);
        connections.remove(txn.getTimestamp()).close();
    }

    @Test
    void dirtyWrite() throws Exception {
        var table = "dirty-write-test";
        var col = Bytes.toBytes("cf");
        storage.createMVCCTable(table, List.of(col)).join();

        var x = Bytes.toBytes("x");
        var y = Bytes.toBytes("y");

        // x = y = 0
        var txn0 = createTxn();
        txn0.put(table, x, col, Bytes.toBytes(0));
        txn0.put(table, y, col, Bytes.toBytes(0));
        assertTrue(txn0.commit());
        releaseTxn(txn0);

        var txn1 = createTxn();
        var txn2 = createTxn();

        // txn1: x = 1, y = 1
        // txn2: x = 2, y = 2
        txn1.put(table, x, col, Bytes.toBytes(1));

        txn2.put(table, y, col, Bytes.toBytes(2));
        txn2.put(table, x, col, Bytes.toBytes(2));
        assertTrue(txn2.commit());
        releaseTxn(txn2);

        txn1.put(table, y, col, Bytes.toBytes(1));
        assertTrue(txn1.commit());
        releaseTxn(txn1);


        var txn3 = createTxn();
        var x3 = txn3.get(table, x, col);
        var y3 = txn3.get(table, y, col);
        assertTrue(Arrays.equals(y3, x3));
        assertTrue(txn3.commit());
        releaseTxn(txn3);

        storage.removeTable(table).join();
    }

    @Test
    void dirtyRead() {
    }

    @Test
    void updateLost() {

    }

    @Test
    void fuzzyRead() {

    }

    @Test
    void phantom() {
    }

    @Test
    void readSkew() {
    }

    @Test
    void writeSkew() {
    }
}
