package org.yangtau.hbs;

public final record KeyValue(Key key, Value value) {
    public final record Key(String table, byte[] row, byte[] col) {
    }

    public final record Value(byte[] value, long timestamp, boolean committed) {
    }
}
