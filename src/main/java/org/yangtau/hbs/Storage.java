package org.yangtau.hbs;

public interface Storage {
    void put(String table, byte[] row, byte[] col, byte[] value) throws Exception;

    byte[] get(String table, byte[] row, byte[] col) throws Exception;

    // check if a row exists
    boolean exists(String table, byte[] row, byte[] col) throws Exception;

    void remove(String table, byte[] row, byte[] col) throws Exception;

    // GET API used by MVTO algorithm:
    // get the newest version in (table, row, col) before `readTimestamp` and update the greatest RT for this version
    byte[] getWithReadTimestamp(String table, byte[] row, byte[] col, long readTimestamp) throws Exception;

    // PUT API used by MVTO algorithm:
    // put if the newest version in (table, row, col) before `writeTimestamp` has less RT than writeTimestamp,
    // thus, there is no conflict between this write and reads before this write
    boolean putIfNoConflict(String table, byte[] row, byte[] col, byte[] value, long writeTimestamp) throws Exception;
}
