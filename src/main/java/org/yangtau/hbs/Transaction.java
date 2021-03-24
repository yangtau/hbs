package org.yangtau.hbs;

public interface Transaction {
    long getTimestamp();

    Status getStatus();

    byte[] get(String table, byte[] row, byte[] col) throws Exception;

    void put(String table, byte[] row, byte[] col, byte[] value) throws Exception;

    boolean commit() throws Exception;

    void abort() throws Exception;

    default void expectUncommitted() throws Exception {
        if (getStatus() != Status.Uncommitted)
            throw new Exception("The transaction is not uncommitted, status: " + getStatus().toString());
    }

    // possible changes:
    // Uncommitted -> Committed
    // Uncommitted -> Aborted
    enum Status {
        Aborted,
        Committed,
        Uncommitted,
    }
}