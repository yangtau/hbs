package org.yangtau.hbs;

public interface TransactionManager extends AutoCloseable {
    // allocate a timestamp, and record it in the TM server (e.g. Zookeeper)
    long allocate() throws Exception;

    // delete the record of the txn in the TM server
    void release(long id) throws Exception;

    // check if a txn exists on the TM server
    boolean exists(long id) throws Exception;

    // wait if a txn exists
    void waitIfExists(long id) throws Exception;
}
