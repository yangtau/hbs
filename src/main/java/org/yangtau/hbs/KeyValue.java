package org.yangtau.hbs;

import java.util.Objects;

public final class KeyValue {
    private final Key key;
    private final Value value;

    public KeyValue(Key key, Value value) {
        this.key = key;
        this.value = value;
    }

    public Key key() {
        return key;
    }

    public Value value() {
        return value;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        KeyValue that = (KeyValue) obj;
        return Objects.equals(this.key, that.key) &&
                Objects.equals(this.value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }

    @Override
    public String toString() {
        return "KeyValue[" +
                "key=" + key + ", " +
                "value=" + value + ']';
    }

    public static final class Value {
        private final byte[] value;
        private final long timestamp;
        private final boolean committed;

        public Value(byte[] value, long timestamp, boolean committed) {
            this.value = value;
            this.timestamp = timestamp;
            this.committed = committed;
        }

        public byte[] value() {
            return value;
        }

        public long timestamp() {
            return timestamp;
        }

        public boolean committed() {
            return committed;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            Value that = (Value) obj;
            return Objects.equals(this.value, that.value) &&
                    this.timestamp == that.timestamp &&
                    this.committed == that.committed;
        }

        @Override
        public int hashCode() {
            return Objects.hash(value, timestamp, committed);
        }

        @Override
        public String toString() {
            return "Value[" +
                    "value=" + value + ", " +
                    "timestamp=" + timestamp + ", " +
                    "committed=" + committed + ']';
        }

    }

    public static final class Key {
        private final String table;
        private final byte[] row;
        private final byte[] column;

        public Key(String table, byte[] row, byte[] column) {
            this.table = table;
            this.row = row;
            this.column = column;
        }

        public String table() {
            return table;
        }

        public byte[] row() {
            return row;
        }

        public byte[] column() {
            return column;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            Key that = (Key) obj;
            return Objects.equals(this.table, that.table) &&
                    Objects.equals(this.row, that.row) &&
                    Objects.equals(this.column, that.column);
        }

        @Override
        public int hashCode() {
            return Objects.hash(table, row, column);
        }

        @Override
        public String toString() {
            return "Key[" +
                    "table=" + table + ", " +
                    "row=" + row + ", " +
                    "column=" + column + ']';
        }
    }
}
