import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.junit.jupiter.api.Test;
import org.yangtau.hbs.TransactionManager;
import org.yangtau.hbs.ZKTransactionManager;

import static org.junit.jupiter.api.Assertions.*;

class ZKTxnManagerTest {

    public static final String connectString = "127.0.0.1";


    void clearParentNode() throws Exception {
        try (
                var client = CuratorFrameworkFactory
                        .newClient(connectString, new RetryForever(100))) {
            client.start();
            if (client.checkExists().forPath("/" + ZKTransactionManager.ZKParentPath) != null) {
                client.delete().forPath("/" + ZKTransactionManager.ZKParentPath);
                ZKTransactionManager.createParentNode(connectString);
            }
        }

    }

    boolean exists(long id) throws Exception {
        try (var client =
                     CuratorFrameworkFactory.newClient(connectString, new RetryForever(100))) {
            client.start();
            return client.checkExists().forPath(
                    "/" + ZKTransactionManager.ZKParentPath + ZKTransactionManager.generateTxnPath(id)
            ) != null;
        }
    }


    @Test
    void createTxnIdTest() throws Exception {
        clearParentNode();

        var len = 10;

        try (TransactionManager manager = new ZKTransactionManager(connectString)) {
            for (long i = 0; i < len; i++) {
                var id = manager.allocate();
                assertEquals(i, id);
            }
        }

        // check that txns are removed after manager closed
        try (TransactionManager manager = new ZKTransactionManager(connectString)) {
            for (long i = 0; i < len; i++) {
                assertFalse(exists(i));
                assertFalse(manager.exists(i));
            }
        }
    }

    @Test
    void releaseTxnIdTest() throws Exception {
        clearParentNode();
        TransactionManager manager = new ZKTransactionManager(connectString);

        var txn1 = manager.allocate();
        var txn2 = manager.allocate();

        // explicitly release
        manager.release(txn1);

        assertTrue(manager.exists(txn2));
        assertFalse(manager.exists(txn1));

        // release txn by close manager
        manager.close();
        assertFalse(manager.exists(txn2));
    }

    @Test
    void waitIfExists1Test() throws Exception {
        clearParentNode();
        TransactionManager manager = new ZKTransactionManager(connectString);

        final long txn1 = manager.allocate();

        var t = new Thread(
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

        // release txn1
        manager.release(txn1);
        manager.close();

        // wait for t
        t.join();
    }

    @Test
    void waitIfExists2Test() throws Exception {
        clearParentNode();
        TransactionManager manager = new ZKTransactionManager(connectString);

        final long txn1 = manager.allocate();

        var t = new Thread(
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

        manager.close();

        // wait for t
        t.join();
    }

    @Test
    void waitIfExists3Test() throws Exception {
        clearParentNode();
        TransactionManager manager = new ZKTransactionManager(connectString);

        final long txn1 = manager.allocate();

        var t = new Thread(
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

        t.interrupt();
        t.join();
        manager.close();
    }
}
