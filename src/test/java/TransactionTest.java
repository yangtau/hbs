import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;
import org.yangtau.hbs.*;
import org.yangtau.hbs.hbase.HBaseStorage;
import org.yangtau.hbs.zookeeper.ZKTransactionManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

public class TransactionTest {
    private AsyncConnection conn;
    private MVCCStorage storage;
    private ConcurrentMap<Long, AsyncConnection> connections;
    private TransactionManager manager;

    void initTest(String table, List<byte[]> cols) {
        conn = ConnectionFactory.createAsyncConnection(HBaseConfiguration.create()).join();
        storage = new HBaseStorage(conn);
        connections = new ConcurrentHashMap<>();

        try {
            storage.removeTable(table).join();
        } catch (Exception ignored) {
        }
        try {
            storage.removeTable(HBSCommitTable.TABLE_NAME).join();
        } catch (CompletionException ignored) {
        }
        try {
            ZKTransactionManager.createParentNode("localhost");
        } catch (Exception ignored) {
        }

        storage.createMVCCTable(table, cols).join();
        storage.createTable(HBSCommitTable.TABLE_NAME, List.of(HBSCommitTable.COLUMN)).join();

        manager = new ZKTransactionManager("localhost");
    }

    void endTest(String table) throws Exception {
        storage.removeTable(table).join();
        conn.close();
        manager.close();
        for (var e : connections.values()) {
            e.close();
        }
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
    }

    void biCheck(KeyValue.Key key1, KeyValue.Key key2, BiPredicate<byte[], byte[]> predicate) throws Exception {
        var txn = createTxn();
        assertTrue(predicate.test(
                txn.get(key1.table(), key1.row(), key1.column()),
                txn.get(key2.table(), key2.row(), key2.column())));
        assertTrue(txn.commit());
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
    }


    @Test
    void dirtyWrite() throws Exception {
        var table = "dirty-write-test";
        var col = Bytes.toBytes("cf");
        initTest(table, List.of(col));

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

        endTest(table);
    }

    @Test
    void dirtyRead() throws Exception {
        var table = "dirty-read-test";
        var col = Bytes.toBytes("cf");
        initTest(table, List.of(col));

        var x = Bytes.toBytes("x");
        var y = Bytes.toBytes("y");

        // x = y = 0
        var txn0 = createTxn();
        txn0.put(table, x, col, Bytes.toBytes(0));
        txn0.put(table, y, col, Bytes.toBytes(0));
        assertTrue(txn0.commit());

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

        endTest(table);
    }

    @Test
    void updateLost() throws Exception {
        var table = "update-lost-test";
        var col = Bytes.toBytes("cf");
        initTest(table, List.of(col));

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

        check(new KeyValue.Key(table, x, col), (v) -> Arrays.equals(v, Bytes.toBytes(2)));
        endTest(table);
    }

    @Test
    void fuzzyRead() throws Exception {
        var table = "fuzzy-read-test";
        var col = Bytes.toBytes("cf");
        initTest(table, List.of(col));

        var x = Bytes.toBytes("x");

        // x = y = 0
        var txn0 = createTxn();
        txn0.put(table, x, col, Bytes.toBytes(0));
        assertTrue(txn0.commit());

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
        endTest(table);
    }

    @Test
    void readSkew() throws Exception {
        var table = "read-skew-test";
        var col = Bytes.toBytes("cf");
        initTest(table, List.of(col));

        var x = Bytes.toBytes("x");
        var y = Bytes.toBytes("y");

        // x = y = 0
        var txn0 = createTxn();
        txn0.put(table, x, col, Bytes.toBytes(0));
        txn0.put(table, y, col, Bytes.toBytes(0));
        assertTrue(txn0.commit());

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
        endTest(table);
    }

    @Test
    void writeSkew() throws Exception {
        var table = "write-skew-test";
        var col = Bytes.toBytes("cf");
        initTest(table, List.of(col));
        var x = Bytes.toBytes("x");
        var y = Bytes.toBytes("y");

        // x = y = 1
        var txn0 = createTxn();
        txn0.put(table, x, col, Bytes.toBytes(1));
        txn0.put(table, y, col, Bytes.toBytes(1));
        assertTrue(txn0.commit());

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
        endTest(table);
    }

    @Test
    void concurrentTxns() throws Exception {
        var table = "balance";
        var col = Bytes.toBytes("data");
        initTest(table, List.of(col));

        List<String> users = new ArrayList<>();
        final int len = 100;
        int sum = 0;
        var random = new Random();

        // create users
        var txn1 = createTxn();
        for (int i = 0; i < len; i++) {
            var b = Math.abs(random.nextInt() % 100);
            var user = "user" + i;
            sum += b;
            users.add(user);
            txn1.put(table, Bytes.toBytes(user), col, Bytes.toBytes(b));
        }
        assertTrue(txn1.commit());
        System.out.println("init txn id: " + txn1.getTimestamp());

        var threads = new ArrayList<Thread>();
        boolean[] committed = new boolean[len];
        for (int i = 0; i < len; i++) {
            final int id = i;
            threads.add(new Thread(() -> {
                // if user1.balance > 20 than user1.balance -= 10; user2.balance += 10
                var user1 = users.get(Math.abs(random.nextInt()) % len);
                var user2 = users.get(Math.abs(random.nextInt()) % len);
                try (var conn = ConnectionFactory.createAsyncConnection(HBaseConfiguration.create()).join();
                     var manager = new ZKTransactionManager("localhost")) {
                    var s = new HBaseStorage(conn);
                    var txn = new HBSTransaction(s, manager, new HBSCommitTable(s));

                    System.out.println("thread " + Thread.currentThread().getId() + " txn id: " + txn.getTimestamp());

                    var user1Balance = Bytes.toInt(txn.get(table, Bytes.toBytes(user1), col));
                    if (user1Balance < 20) {
                        txn.abort();
                        committed[id] = true;
                        return;
                    }

                    txn.put(table, Bytes.toBytes(user1), col, Bytes.toBytes(user1Balance - 10));
                    var user2Balance = Bytes.toInt(txn.get(table, Bytes.toBytes(user2), col));
                    if (user1.equals(user2)) {
                        assertEquals(user1Balance - 10, user2Balance);
                    }
                    txn.put(table, Bytes.toBytes(user2), col, Bytes.toBytes(user2Balance + 10));

                    if (committed[id] = txn.commit())
                        System.out.println("thread " + Thread.currentThread().getId() + " " + user1 + " -> " + user2);
                } catch (Exception e) {
                    e.printStackTrace(System.err);
                }
            }));
        }

        for (var t : threads)
            t.start();

        for (var t : threads)
            t.join();

        int committedCount = 0;
        for (int i = 0; i < len; i++)
            if (committed[i]) committedCount++;
        System.out.println("# committed: " + committedCount);

        // check consistency
        var txn2 = createTxn();
        int newSum = 0;
        for (var u : users) {
            newSum += Bytes.toInt(txn2.get(table, Bytes.toBytes(u), col));
        }
        txn2.commit();
        assertEquals(sum, newSum);

        endTest(table);
    }
}
