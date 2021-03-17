package org.yangtau.hbs;

public interface Transaction {
    long getTimestamp();

    byte[] get(String table, byte[] row, byte[] col) throws Exception;

    void put(String table, byte[] row, byte[] col, byte[] value) throws Exception;

    boolean commit() throws Exception;

    void abort() throws Exception;
}