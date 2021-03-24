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
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

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

    void check(KeyValue.Key key, Predicate<byte[]> predicate) throws Exception {
        var txn = createTxn();
        assertTrue(predicate.test(
                txn.get(key.table(), key.row(), key.column())
        ));
        assertTrue(txn.commit());
        releaseTxn(txn);
    }

    void biCheck(KeyValue.Key key1, KeyValue.Key key2, BiPredicate<byte[], byte[]> predicate) throws Exception {
        var txn = createTxn();
        assertTrue(predicate.test(
                txn.get(key1.table(), key1.row(), key1.column()),
                txn.get(key2.table(), key2.row(), key2.column())));
        assertTrue(txn.commit());
        releaseTxn(txn);
    }

    void checkEqual(String table, byte[] col, byte[] x, byte[] y) throws Exception {
        biCheck(new KeyValue.Key(table, x, col),
                new KeyValue.Key(table, y, col),
                Arrays::equals);
    }

    // txn1: x = 1, y = 1
    // txn2: x = 2, y = 2
    // txn2 commit before txn1
    void dirtyWriteCase1(String table, byte[] col, byte[] x, byte[] y) throws Exception {
        var txn1 = createTxn();
        var txn2 = createTxn();
        txn1.put(table, x, col, Bytes.toBytes(1));

        txn2.put(table, y, col, Bytes.toBytes(2));
        txn2.put(table, x, col, Bytes.toBytes(2));
        assertTrue(txn2.commit());

        txn1.put(table, y, col, Bytes.toBytes(1));
        assertTrue(txn1.commit());

        releaseTxn(txn2);
        releaseTxn(txn1);
    }

    // txn2 abort before txn1 commit
    void dirtyWriteCase2(String table, byte[] col, byte[] x, byte[] y) throws Exception {
        var txn1 = createTxn();
        var txn2 = createTxn();
        txn1.put(table, x, col, Bytes.toBytes(1));

        txn2.put(table, y, col, Bytes.toBytes(2));
        txn2.put(table, x, col, Bytes.toBytes(2));
        txn2.abort();

        txn1.put(table, y, col, Bytes.toBytes(1));
        assertTrue(txn1.commit());

        releaseTxn(txn2);
        releaseTxn(txn1);
    }


    @Test
    void dirtyWrite() throws Exception {
        var table = "dirty-write-test";
        var col = Bytes.toBytes("cf");
        try {
            storage.removeTable(table).join();
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
        storage.createMVCCTable(table, List.of(col)).join();

        var x = Bytes.toBytes("x");
        var y = Bytes.toBytes("y");

        // x = y = 0
        var txn0 = createTxn();
        txn0.put(table, x, col, Bytes.toBytes(0));
        txn0.put(table, y, col, Bytes.toBytes(0));
        assertTrue(txn0.commit());
        releaseTxn(txn0);

        dirtyWriteCase1(table, col, x, y);
        checkEqual(table, col, x, y);

        dirtyWriteCase2(table, col, x, y);
        checkEqual(table, col, x, y);

        storage.removeTable(table).join();
    }

    @Test
    void dirtyRead() throws Exception {
        var table = "dirty-read-test";
        var col = Bytes.toBytes("cf");
        try {
            storage.removeTable(table).join();
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
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
        txn1.put(table, x, col, Bytes.toBytes(1));
        txn1.put(table, y, col, Bytes.toBytes(2));

        // txn2 should get the data written by txn0
        var x2 = txn2.get(table, x, col);
        var y2 = txn2.get(table, y, col);
        assertTrue(Arrays.equals(Bytes.toBytes(0), x2));
        assertTrue(Arrays.equals(Bytes.toBytes(0), y2));

        // txn1 would fail to commit!
        assertFalse(txn1.commit());

        assertTrue(txn2.commit());

        releaseTxn(txn1);
        releaseTxn(txn2);

        storage.removeTable(table).join();
    }

    @Test
    void updateLost() throws Exception {
        var table = "update-lost-test";
        var col = Bytes.toBytes("cf");
        try {
            storage.removeTable(table).join();
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
        storage.createMVCCTable(table, List.of(col)).join();

        var x = Bytes.toBytes("x");

        // x = y = 0
        var txn0 = createTxn();
        txn0.put(table, x, col, Bytes.toBytes(0));
        assertTrue(txn0.commit());
        releaseTxn(txn0);

        // x = x + 1
        var txn1 = createTxn();
        // x = x + 10
        var txn2 = createTxn();

        var txn1x = Bytes.toInt(txn1.get(table, x, col));
        var txn2x = Bytes.toInt(txn2.get(table, x, col));

        txn1.put(table, x, col, Bytes.toBytes(1 + txn1x));
        txn2.put(table, x, col, Bytes.toBytes(2 + txn2x));

        assertTrue(txn2.commit());

        // txn1 should fail to commit
        assertFalse(txn1.commit());

        releaseTxn(txn1);
        releaseTxn(txn2);

        check(new KeyValue.Key(table, x, col), (v) -> Arrays.equals(v, Bytes.toBytes(2)));
        storage.removeTable(table).join();
    }

    @Test
    void fuzzyRead() throws Exception {
        var table = "fuzzy-read-test";
        var col = Bytes.toBytes("cf");
        try {
            storage.removeTable(table).join();
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
        storage.createMVCCTable(table, List.of(col)).join();

        var x = Bytes.toBytes("x");

        // x = y = 0
        var txn0 = createTxn();
        txn0.put(table, x, col, Bytes.toBytes(0));
        assertTrue(txn0.commit());
        releaseTxn(txn0);

        // x = 10
        var txn1 = createTxn();
        // read x twice
        var txn2 = createTxn();


        var txn1x1 = Bytes.toInt(txn1.get(table, x, col));

        txn2.put(table, x, col, Bytes.toBytes(10));
        assertTrue(txn2.commit());

        var txn1x2 = Bytes.toInt(txn1.get(table, x, col));

        assertEquals(txn1x1, txn1x2);

        assertTrue(txn1.commit());

        releaseTxn(txn1);
        releaseTxn(txn2);

        storage.removeTable(table).join();
    }

    @Test
    void readSkew() throws Exception {
        var table = "read-skew-test";
        var col = Bytes.toBytes("cf");
        try {
            storage.removeTable(table).join();
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
        storage.createMVCCTable(table, List.of(col)).join();

        var x = Bytes.toBytes("x");
        var y = Bytes.toBytes("y");

        // x = y = 0
        var txn0 = createTxn();
        txn0.put(table, x, col, Bytes.toBytes(0));
        txn0.put(table, y, col, Bytes.toBytes(0));
        assertTrue(txn0.commit());
        releaseTxn(txn0);

        // read x, read y
        var txn1 = createTxn();
        // write x = 1, write y = 1
        var txn2 = createTxn();

        var txn1x = txn1.get(table, x, col);
        txn2.put(table, x, col, Bytes.toBytes(1));
        txn2.put(table, y, col, Bytes.toBytes(1));
        var txn1y = txn1.get(table, y, col);

        assertTrue(txn2.commit());
        assertTrue(txn1.commit());

        assertTrue(Arrays.equals(txn1x, txn1y));
        biCheck(new KeyValue.Key(table, x, col), new KeyValue.Key(table, y, col),
                (v1, v2) -> Arrays.equals(v1, Bytes.toBytes(1)) && Arrays.equals(v2, Bytes.toBytes(1)));

        releaseTxn(txn1);
        releaseTxn(txn2);

        storage.removeTable(table).join();
    }

    @Test
    void writeSkew() throws Exception {
        var table = "write-skew-test";
        var col = Bytes.toBytes("cf");
        try {
            storage.removeTable(table).join();
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
        storage.createMVCCTable(table, List.of(col)).join();

        var x = Bytes.toBytes("x");
        var y = Bytes.toBytes("y");

        // x = y = 1
        var txn0 = createTxn();
        txn0.put(table, x, col, Bytes.toBytes(1));
        txn0.put(table, y, col, Bytes.toBytes(1));
        assertTrue(txn0.commit());
        releaseTxn(txn0);

        // txn1: if x+y == 2 then x = 0
        var txn1 = createTxn();
        // txn1: if x+y == 2 then y = 0
        var txn2 = createTxn();

        var txn1x = txn1.get(table, x, col);
        var txn1y = txn1.get(table, y, col);
        var txn2x = txn2.get(table, x, col);
        var txn2y = txn2.get(table, y, col);

        if (Bytes.toInt(txn1x) + Bytes.toInt(txn1y) == 2)
            txn1.put(table, x, col, Bytes.toBytes(0));

        if (Bytes.toInt(txn2x) + Bytes.toInt(txn2y) == 2)
            txn2.put(table, x, col, Bytes.toBytes(0));

        assertTrue(txn2.commit());
        assertFalse(txn1.commit());

        biCheck(new KeyValue.Key(table, x, col), new KeyValue.Key(table, y, col),
                (v1, v2) -> Bytes.toInt(v1) + Bytes.toInt(v2) == 1);

        releaseTxn(txn1);
        releaseTxn(txn2);
        storage.removeTable(table).join();
    }
}
