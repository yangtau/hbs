import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.junit.jupiter.api.Test;
import org.yangtau.hbs.TransactionManager;
import org.yangtau.hbs.zookeeper.ZKTransactionManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class ZKTxnManagerTest {
    public static final String connectString = "127.0.0.1";

    void createParentNode() throws Exception {
        try (CuratorFramework client = CuratorFrameworkFactory
                .newClient(connectString, new RetryForever(100))) {
            client.start();
            if (client.checkExists().forPath("/" + ZKTransactionManager.ZKParentPath) == null) {
                ZKTransactionManager.createParentNode(connectString);
            }
        }
    }

    boolean exists(long id) throws Exception {
        try (CuratorFramework client =
                     CuratorFrameworkFactory.newClient(connectString, new RetryForever(100))) {
            client.start();
            return client.checkExists().forPath(
                    "/" + ZKTransactionManager.ZKParentPath + ZKTransactionManager.generateTxnPath(id)
            ) != null;
        }
    }


    @Test
    void createTxnId() throws Exception {
        createParentNode();

        long len = 100;
        long start;

        try (TransactionManager manager = new ZKTransactionManager(connectString)) {
            start = manager.allocate();
            for (long i = 0; i < len; i++) {
                long id = manager.allocate();
                assertEquals(i + 1 + start, id);
            }
        }

        // check that txns are removed after manager closed
        try (TransactionManager manager = new ZKTransactionManager(connectString)) {
            for (long i = 0; i < len; i++) {
                assertFalse(exists(i + 1 + start));
                assertFalse(manager.exists(i + 1 + start));
            }
        }
    }

    @Test
    void concurrentCreate() throws Exception {
        createParentNode();

        TransactionManager manager = new ZKTransactionManager(connectString);

        long len = 50;
        final long start = manager.allocate();

        List<Thread> list = new ArrayList<Thread>();
        Runnable worker = () -> {
                try (TransactionManager m = new ZKTransactionManager(connectString)) {
                    long id = m.allocate();
                    assertTrue(id > start);
                    Thread.sleep(Math.abs(new Random().nextInt() % 200));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            };

        for (long i = 0; i < len; i++) {
            list.add(new Thread(worker));
        }

        for (Thread t : list)
            t.start();

        for (Thread t : list) {
            t.join();
        }

        for (int i = 0; i < len; i++) {
            long id = manager.allocate();
            assertEquals(len + i + 1 + start, id);
        }
        manager.close();
    }

    @Test
    void releaseTxnId() throws Exception {
        createParentNode();
        TransactionManager manager = new ZKTransactionManager(connectString);

        long txn1 = manager.allocate();
        long txn2 = manager.allocate();

        // explicitly release
        manager.release(txn1);

        assertTrue(manager.exists(txn2));
        assertFalse(manager.exists(txn1));

        // release txn by close manager
        manager.close();
        assertFalse(exists(txn2));
    }

    @Test
    void waitIfExists() throws Exception {
        createParentNode();
        TransactionManager manager = new ZKTransactionManager(connectString);

        final long txn1 = manager.allocate();

        Thread t = new Thread(
                () -> {
                    // wait for txn1
                    TransactionManager txnManager = new ZKTransactionManager(connectString);
                    try {
                        txnManager.waitIfExists(txn1);
                        assertFalse(exists(txn1));
                        txnManager.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
        );

        t.start();

        Thread.sleep(2000);

        // release txn1
        manager.release(txn1);
        manager.close();

        // wait for t
        t.join();
    }


    @Test
    void waitForever() throws Exception {
        createParentNode();
        TransactionManager manager = new ZKTransactionManager(connectString);

        final long txn1 = manager.allocate();

        Thread t = new Thread(
                () -> {
                    try {
                        TransactionManager txnManager = new ZKTransactionManager(connectString);
                        try {
                            // should wait forever
                            txnManager.waitIfExists(txn1);
                            fail();
                        } catch (InterruptedException e) {
                            assertTrue(exists(txn1));
                        }

                        txnManager.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
        );

        t.start();
        Thread.sleep(2000);

        t.interrupt();

        t.join();
        manager.close();
    }

    @Test
    void waitIfNotExist() throws Exception {
        createParentNode();

        final long txn1 = 0;
        Thread t = new Thread(
                () -> {
                    try {
                        TransactionManager txnManager = new ZKTransactionManager(connectString);
                        // should not wait
                        txnManager.waitIfExists(txn1);
                        assertFalse(txnManager.exists(txn1));
                        txnManager.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
        );
        t.start();
        t.join();
    }
}
