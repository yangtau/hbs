package org.yangtau.hbs.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.yangtau.hbs.TransactionManager;

public class ZKTransactionManager implements TransactionManager {
    public static final String ZKParentPath = "hbs";
    public static final String ZKPrefix = "/txn-";
    /*
    TODO: the counter may overflow
    the counter used to store the next sequence number is a signed int (4bytes) maintained by the parent node
     */
    private static final String ZKCounterFormat = "%010d";
    private static final RetryPolicy retryPolicy = new RetryUntilElapsed(5000, 100);

    CuratorFramework client;


    public ZKTransactionManager(String connectionStrings) {
        client = CuratorFrameworkFactory.builder()
                .namespace(ZKParentPath)
                .connectString(connectionStrings)
                .retryPolicy(retryPolicy)
                .build();

        client.start();
    }

    public static void createParentNode(String connectionStrings) throws Exception {
        try (var client = CuratorFrameworkFactory.newClient(connectionStrings, retryPolicy)) {
            client.start();
            client.create().forPath("/" + ZKParentPath);
        }
    }

    // converts txn path in zookeeper to txn id
    public static long parseTxnId(String txn) throws Exception {
        // "txn-xxxx"
        var ss = txn.split("-");
        if (ss.length != 2) throw new Exception("unexpected transaction path: " + txn);
        return Integer.parseInt(ss[1]);
    }

    // converts txn id to txn path in zookeeper
    public static String generateTxnPath(long id) {
        return String.format(ZKPrefix + ZKCounterFormat, id);
    }

    @Override
    public void close() {
        client.close();
    }

    @Override
    public long allocate() throws Exception {
        // FIXME: sometimes lower number has been allocated
        var res = client.create()
                .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                .forPath(ZKPrefix);
        return parseTxnId(res);
    }

    @Override
    public void release(long id) throws Exception {
        client.delete().forPath(generateTxnPath(id));
    }

    @Override
    public boolean exists(long id) throws Exception {
        return client.checkExists().forPath(generateTxnPath(id)) != null;
    }

    @Override
    public void waitIfExists(long id) throws Exception {
        Object sync = new Object();
        CuratorWatcher watcher = event -> {
            if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
                synchronized (sync) {
                    sync.notify();
                }
            }
        };

        if (client.checkExists().usingWatcher(watcher).forPath(generateTxnPath(id)) == null)
            return;

        // keep waiting util the txn is removed
        synchronized (sync) {
            sync.wait();
        }
    }
}
