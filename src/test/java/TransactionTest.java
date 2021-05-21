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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
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
        for (AsyncConnection e : connections.values()) {
            e.close();
        }
    }

    Transaction createTxn() throws Exception {
        AsyncConnection con = ConnectionFactory
                .createAsyncConnection(HBaseConfiguration.create()).join();

        MVCCStorage s = new HBaseStorage(con);
        Transaction txn = new HBSTransaction(s,
                manager,
                new HBSCommitTable(s));

        connections.put(txn.getTimestamp(), con);
        return txn;
    }

    void releaseTxn(Transaction txn) throws IOException {
        connections.remove(txn.getTimestamp()).close();
    }

    void check(KeyValue.Key key, Predicate<byte[]> predicate) throws Exception {
        Transaction txn = createTxn();
        assertTrue(predicate.test(
                txn.get(key.table(), key.row(), key.column())
        ));
        assertTrue(txn.commit());
    }

    void biCheck(KeyValue.Key key1, KeyValue.Key key2, BiPredicate<byte[], byte[]> predicate) throws Exception {
        Transaction txn = createTxn();
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
        Transaction txn1 = createTxn();
        Transaction txn2 = createTxn();
        txn1.put(table, x, col, Bytes.toBytes(1));

        txn2.put(table, y, col, Bytes.toBytes(2));
        txn2.put(table, x, col, Bytes.toBytes(2));
        assertTrue(txn2.commit());

        txn1.put(table, y, col, Bytes.toBytes(1));
        assertTrue(txn1.commit());
    }

    // txn2 abort before txn1 commit
    void dirtyWriteCase2(String table, byte[] col, byte[] x, byte[] y) throws Exception {
        Transaction txn1 = createTxn();
        Transaction txn2 = createTxn();
        txn1.put(table, x, col, Bytes.toBytes(1));

        txn2.put(table, y, col, Bytes.toBytes(2));
        txn2.put(table, x, col, Bytes.toBytes(2));
        txn2.abort();

        txn1.put(table, y, col, Bytes.toBytes(1));
        assertTrue(txn1.commit());
    }

    @Test
    void dirtyWrite() throws Exception {
        String table = "dirty-write-test";
        byte[] col = Bytes.toBytes("cf");
        initTest(table, List.of(col));

        byte[] x = Bytes.toBytes("x");
        byte[] y = Bytes.toBytes("y");

        // x = y = 0
        Transaction txn0 = createTxn();
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
        String table = "dirty-read-test";
        byte[] col = Bytes.toBytes("cf");
        initTest(table, List.of(col));

        byte[] x = Bytes.toBytes("x");
        byte[] y = Bytes.toBytes("y");

        // x = y = 0
        Transaction txn0 = createTxn();
        txn0.put(table, x, col, Bytes.toBytes(0));
        txn0.put(table, y, col, Bytes.toBytes(0));
        assertTrue(txn0.commit());

        Transaction txn1 = createTxn();
        Transaction txn2 = createTxn();
        txn1.put(table, x, col, Bytes.toBytes(1));
        txn1.put(table, y, col, Bytes.toBytes(2));

        // txn2 should get the data written by txn0
        byte[] x2 = txn2.get(table, x, col);
        byte[] y2 = txn2.get(table, y, col);
        assertTrue(Arrays.equals(Bytes.toBytes(0), x2));
        assertTrue(Arrays.equals(Bytes.toBytes(0), y2));

        // txn1 would fail to commit!
        assertFalse(txn1.commit());

        assertTrue(txn2.commit());

        endTest(table);
    }

    @Test
    void updateLost() throws Exception {
        String table = "update-lost-test";
        byte[] col = Bytes.toBytes("cf");
        initTest(table, List.of(col));

        byte[] x = Bytes.toBytes("x");

        // x = y = 0
        Transaction txn0 = createTxn();
        txn0.put(table, x, col, Bytes.toBytes(0));
        assertTrue(txn0.commit());
        releaseTxn(txn0);

        // x = x + 1
        Transaction txn1 = createTxn();
        // x = x + 10
        Transaction txn2 = createTxn();

        int txn1x = Bytes.toInt(txn1.get(table, x, col));
        int txn2x = Bytes.toInt(txn2.get(table, x, col));

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
        String table = "fuzzy-read-test";
        byte[] col = Bytes.toBytes("cf");
        initTest(table, List.of(col));

        byte[] x = Bytes.toBytes("x");

        // x = y = 0
        Transaction txn0 = createTxn();
        txn0.put(table, x, col, Bytes.toBytes(0));
        assertTrue(txn0.commit());

        // x = 10
        Transaction txn1 = createTxn();
        // read x twice
        Transaction txn2 = createTxn();


        int txn1x1 = Bytes.toInt(txn1.get(table, x, col));

        txn2.put(table, x, col, Bytes.toBytes(10));
        assertTrue(txn2.commit());

        int txn1x2 = Bytes.toInt(txn1.get(table, x, col));

        assertEquals(txn1x1, txn1x2);

        assertTrue(txn1.commit());
        endTest(table);
    }

    @Test
    void readSkew() throws Exception {
        String table = "read-skew-test";
        byte[] col = Bytes.toBytes("cf");
        initTest(table, List.of(col));

        byte[] x = Bytes.toBytes("x");
        byte[] y = Bytes.toBytes("y");

        // x = y = 0
        Transaction txn0 = createTxn();
        txn0.put(table, x, col, Bytes.toBytes(0));
        txn0.put(table, y, col, Bytes.toBytes(0));
        assertTrue(txn0.commit());

        // write x = 1, write y = 1
        Transaction txn1 = createTxn();
        // read x, read y
        Transaction txn2 = createTxn();

        byte[] txn1x = txn2.get(table, x, col);
        txn1.put(table, x, col, Bytes.toBytes(1));
        txn1.put(table, y, col, Bytes.toBytes(1));
        byte[] txn1y = txn2.get(table, y, col);

        assertFalse(txn1.commit());
        assertTrue(txn2.commit());

        assertTrue(Arrays.equals(txn1x, txn1y));
        biCheck(new KeyValue.Key(table, x, col), new KeyValue.Key(table, y, col),
                (v1, v2) -> Arrays.equals(v1, Bytes.toBytes(0)) && Arrays.equals(v2, Bytes.toBytes(0)));
        endTest(table);
    }

    @Test
    void writeSkew() throws Exception {
        String table = "write-skew-test";
        byte[] col = Bytes.toBytes("cf");
        initTest(table, List.of(col));
        byte[] x = Bytes.toBytes("x");
        byte[] y = Bytes.toBytes("y");

        // x = y = 1
        Transaction txn0 = createTxn();
        txn0.put(table, x, col, Bytes.toBytes(1));
        txn0.put(table, y, col, Bytes.toBytes(1));
        assertTrue(txn0.commit());

        // txn1: if x+y == 2 then x = 0
        Transaction txn1 = createTxn();
        // txn2: if x+y == 2 then y = 0
        Transaction txn2 = createTxn();

        byte[] txn1x = txn1.get(table, x, col);
        byte[] txn1y = txn1.get(table, y, col);
        byte[] txn2x = txn2.get(table, x, col);
        byte[] txn2y = txn2.get(table, y, col);

        if (Bytes.toInt(txn1x) + Bytes.toInt(txn1y) == 2)
            txn1.put(table, x, col, Bytes.toBytes(0));

        if (Bytes.toInt(txn2x) + Bytes.toInt(txn2y) == 2)
            txn2.put(table, y, col, Bytes.toBytes(0));

        assertTrue(txn2.commit());
        assertFalse(txn1.commit());

        biCheck(new KeyValue.Key(table, x, col), new KeyValue.Key(table, y, col),
                (v1, v2) -> Bytes.toInt(v1) + Bytes.toInt(v2) == 1);
        endTest(table);
    }

    @Test
    void concurrentWriteSkew() throws Exception {
        String table = "con-write-skew-test";
        byte[] col = Bytes.toBytes("cf");
        initTest(table, List.of(col));
        byte[] x = Bytes.toBytes("x");
        byte[] y = Bytes.toBytes("y");

        int length = 100;

        // x = y = 1
        Transaction txn0 = createTxn();
        txn0.put(table, x, col, Bytes.toBytes(1));
        txn0.put(table, y, col, Bytes.toBytes(1));
        assertTrue(txn0.commit());

        AtomicInteger committedCounter = new AtomicInteger(0);
        // if x+y == 2 then x = 0
        Runnable txnRunnable1 = () -> {
            try (AsyncConnection conn = ConnectionFactory.createAsyncConnection(HBaseConfiguration.create()).join();
                 TransactionManager manger = new ZKTransactionManager("localhost")) {
                MVCCStorage s = new HBaseStorage(conn);
                Transaction txn = new HBSTransaction(s, manager, new HBSCommitTable(s));
                if (Bytes.toInt(txn.get(table, x, col)) + Bytes.toInt(txn.get(table, y, col)) == 2) {
                    txn.put(table, x, col, Bytes.toBytes(0));
                    if (txn.commit())
                        committedCounter.incrementAndGet();
                } else {
                    txn.abort();
                    committedCounter.incrementAndGet();
                }
            } catch (Exception e) {
                e.printStackTrace(System.err);
            }
        };
        //  if x+y == 2 then y = 0
        Runnable txnRunnable2 = () -> {
            try (AsyncConnection conn = ConnectionFactory.createAsyncConnection(HBaseConfiguration.create()).join();
                 TransactionManager manger = new ZKTransactionManager("localhost")) {
                MVCCStorage s = new HBaseStorage(conn);
                Transaction txn = new HBSTransaction(s, manager, new HBSCommitTable(s));
                if (Bytes.toInt(txn.get(table, x, col)) + Bytes.toInt(txn.get(table, y, col)) == 2) {
                    txn.put(table, y, col, Bytes.toBytes(0));
                    if (txn.commit())
                        committedCounter.incrementAndGet();
                } else {
                    txn.abort();
                    committedCounter.incrementAndGet();
                }
            } catch (Exception e) {
                e.printStackTrace(System.err);
            }
        };

        // if x == 0 then x = 1
        Runnable txnRunnable3 = () -> {
            try (AsyncConnection conn = ConnectionFactory.createAsyncConnection(HBaseConfiguration.create()).join();
                 TransactionManager manger = new ZKTransactionManager("localhost")) {
                MVCCStorage s = new HBaseStorage(conn);
                Transaction txn = new HBSTransaction(s, manager, new HBSCommitTable(s));
                if (Bytes.toInt(txn.get(table, x, col)) == 0) {
                    txn.put(table, x, col, Bytes.toBytes(1));
                    if (txn.commit())
                        committedCounter.incrementAndGet();
                } else {
                    txn.abort();
                    committedCounter.incrementAndGet();
                }
            } catch (Exception e) {
                e.printStackTrace(System.err);
            }
        };

        // if y == 0 then y = 1
        Runnable txnRunnable4 = () -> {
            try (AsyncConnection conn = ConnectionFactory.createAsyncConnection(HBaseConfiguration.create()).join();
                 TransactionManager manger = new ZKTransactionManager("localhost")) {
                MVCCStorage s = new HBaseStorage(conn);
                Transaction txn = new HBSTransaction(s, manager, new HBSCommitTable(s));
                if (Bytes.toInt(txn.get(table, y, col)) == 0) {
                    txn.put(table, y, col, Bytes.toBytes(1));
                    if (txn.commit())
                        committedCounter.incrementAndGet();
                } else {
                    txn.abort();
                    committedCounter.incrementAndGet();
                }
            } catch (Exception e) {
                e.printStackTrace(System.err);
            }
        };

        // check x + y == 1 or x + y == 2
        Runnable txnRunnable5 = () -> {
            try (AsyncConnection conn = ConnectionFactory.createAsyncConnection(HBaseConfiguration.create()).join();
                 TransactionManager manger = new ZKTransactionManager("localhost")) {
                MVCCStorage s = new HBaseStorage(conn);
                Transaction txn = new HBSTransaction(s, manager, new HBSCommitTable(s));
                int res = Bytes.toInt(txn.get(table, x, col)) + Bytes.toInt(txn.get(table, y, col));
                assertTrue(res == 1 || res == 2);
                assertTrue(txn.commit());
                committedCounter.incrementAndGet();
            } catch (Exception e) {
                e.printStackTrace(System.err);
            }
        };

        Runnable[] runnables = {txnRunnable1, txnRunnable2, txnRunnable3, txnRunnable4, txnRunnable5};


        List<Thread> threads = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            Thread t = new Thread(runnables[Math.abs(new Random().nextInt() % 5)]);
            threads.add(t);
            t.start();
        }

        for (Thread t : threads) {
            t.join();
        }

        System.out.println("#committed: " + committedCounter);
        endTest(table);
    }

    @Test
    void concurrentTxns() throws Exception {
        String table = "balance";
        byte[] col = Bytes.toBytes("data");
        initTest(table, List.of(col));

        List<String> users = new ArrayList<>();
        final int len = 1000;
        final int userCount = 100;
        int sum = 0;
        Random random = new Random();

        ExecutorService pool = Executors.newFixedThreadPool(100);

        // create users
        Transaction txn1 = createTxn();
        for (int i = 0; i < userCount; i++) {
            int b = Math.abs(random.nextInt() % 100);
            String user = "user" + i;
            sum += b;
            users.add(user);
            txn1.put(table, Bytes.toBytes(user), col, Bytes.toBytes(b));
        }
        assertTrue(txn1.commit());

        AtomicInteger counter = new AtomicInteger(0);
        List<Callable<Void>> callables = new ArrayList<>(len);
        for (int i = 0; i < len; i++) {
            callables.add(() -> {
                // if user1.balance > 20 than user1.balance -= 10; user2.balance += 10
                String user1 = users.get(Math.abs(random.nextInt()) % userCount);
                String user2 = users.get(Math.abs(random.nextInt()) % userCount);
                try (AsyncConnection conn = ConnectionFactory.createAsyncConnection(HBaseConfiguration.create()).join();
                     TransactionManager manger = new ZKTransactionManager("localhost")) {
                    MVCCStorage s = new HBaseStorage(conn);
                    Transaction txn = new HBSTransaction(s, manager, new HBSCommitTable(s));

                    int user1Balance = Bytes.toInt(txn.get(table, Bytes.toBytes(user1), col));
                    if (user1Balance < 20) {
                        txn.abort();
                        counter.incrementAndGet();
                        return null;
                    }

                    txn.put(table, Bytes.toBytes(user1), col, Bytes.toBytes(user1Balance - 10));
                    int user2Balance = Bytes.toInt(txn.get(table, Bytes.toBytes(user2), col));
                    if (user1.equals(user2)) {
                        assertEquals(user1Balance - 10, user2Balance);
                    }
                    txn.put(table, Bytes.toBytes(user2), col, Bytes.toBytes(user2Balance + 10));

                    if (txn.commit()) {
                        System.out.println("thread " + Thread.currentThread().getId() + " " + user1 + " -> " + user2);
                        counter.incrementAndGet();
                    }
                } catch (Exception e) {
                    e.printStackTrace(System.err);
                }
                return null;
            });
        }

        for (Future<Void> f : pool.invokeAll(callables))
            f.get();

        System.out.println("#committed: " + counter);

        // check consistency
        Transaction txn2 = createTxn();
        int newSum = 0;
        for (String u : users) {
            newSum += Bytes.toInt(txn2.get(table, Bytes.toBytes(u), col));
        }
        txn2.commit();
        assertEquals(sum, newSum);

        endTest(table);
    }
}
