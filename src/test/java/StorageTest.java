import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;
import org.yangtau.hbs.KeyValue;
import org.yangtau.hbs.MVCCStorage;
import org.yangtau.hbs.Storage;
import org.yangtau.hbs.hbase.Constants;
import org.yangtau.hbs.hbase.HBaseStorage;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StorageTest {
    private final Configuration configuration = HBaseConfiguration.create();

    private void removeTableIfExists(AsyncConnection conn, String table) {
        var admin = conn.getAdmin();
        admin.listTableNames().thenComposeAsync((list) -> {
            for (var t : list) {
                if (t.getNameAsString().equals(table))
                    return admin.disableTable(t)
                            .thenComposeAsync((v) -> admin.deleteTable(t));
            }
            return CompletableFuture.runAsync(() -> {
            });
        }).join();
    }

    @Test
    void singleVersion() throws IOException {
        try (var conn = ConnectionFactory.createAsyncConnection(configuration).join()) {
            Storage s = new HBaseStorage(conn);
            var table = "HELLO";

            removeTableIfExists(conn, table);

            var col1 = Bytes.toBytes("CF");
            var col2 = Bytes.toBytes("ANOTHER CF");
            s.createTable(table, List.of(col1, col2)).join();

            var row1 = Bytes.toBytes("row1");
            var val1 = Bytes.toBytes("hello world");
            var key1 = new KeyValue.Key(table, row1, col1);

            assertFalse(s.exists(key1).join());
            // put
            s.put(key1, val1).join();
            // exists
            assertTrue(s.exists(key1).join());
            // get
            assertTrue(Arrays.equals(val1, s.get(key1).join()));
            // remove
            s.remove(key1).join();
            // exists
            assertFalse(s.exists(key1).join(), Bytes.toString(s.get(key1).join()));

            s.removeTable(table).join();
        }
    }

    @Test
    void singleRowMultiVersion() throws Exception {
        try (var conn = ConnectionFactory.createAsyncConnection(configuration).join()) {
            MVCCStorage s = new HBaseStorage(conn);
            var table = "HELLO";

            removeTableIfExists(conn, table);

            var col1 = Bytes.toBytes("CF");
            var col2 = Bytes.toBytes("ANOTHER CF");
            s.createMVCCTable(table, List.of(col1, col2)).join();

            var row1 = Bytes.toBytes("row1");
            var val1 = Bytes.toBytes("hello world");
            var key1 = new KeyValue.Key(table, row1, col1);

            assertFalse(s.exists(key1).join());

            for (long ts = 0; ts < 10; ts++) {
                // put
                assertTrue(s.putIfNoConflict(key1, val1, ts).join());
                // get
                assertTrue(Arrays.equals(val1, s.get(key1).join()));

                // clean flags
                s.cleanUncommittedFlag(key1, ts).join();
                // exists
                assertTrue(s.exists(key1).join());

                checkNoCommittedFlag(conn, table, key1, ts);

                s.removeCell(key1, ts).join();
                assertFalse(s.exists(key1).join());
            }

            s.removeTable(table).join();
        }
    }

    @Test
    void multiRowMultiVersion() throws Exception {
        try (var conn = ConnectionFactory.createAsyncConnection(configuration).join()) {
            MVCCStorage s = new HBaseStorage(conn);
            var table1 = "HELLO";
            var table2 = "WORLD";

            removeTableIfExists(conn, table1);
            removeTableIfExists(conn, table2);

            var col1 = Bytes.toBytes("CF");
            var col2 = Bytes.toBytes("ANOTHER CF");
            s.createMVCCTable(table1, List.of(col1, col2)).join();
            s.createMVCCTable(table2, List.of(col1, col2)).join();

            Map<KeyValue.Key, KeyValue.Value> map = new HashMap<>();
            var random = new Random();

            long ts = Math.abs(random.nextLong()) % 20;
            for (int i = 0; i < 20; i++) {
                var row = Bytes.toBytes("row:" + random.nextBoolean());
                var value = Bytes.toBytes("value:" + random.nextBoolean());
                var table = random.nextInt() % 2 == 0 ? table1 : table2;
                var col = random.nextInt() % 2 == 0 ? col1 : col2;
                var key = new KeyValue.Key(table, row, col);
                var val = new KeyValue.Value(value, ts, false);

                if (!map.containsKey(key)) {
                    map.put(key, val);
                    assertTrue(s.putIfNoConflict(key, value, ts).join());
                    assertTrue(s.exists(key).join());
                }
            }

            // clean all uncommitted flags
            s.cleanUncommittedFlags(map.keySet(), ts).join();
            for (var k : map.keySet()) {
                checkNoCommittedFlag(conn, k.table(), k, ts);
            }

            // remove all
            s.removeCells(map.keySet(), ts).get();
            for (var k : map.keySet()) {
                assertFalse(exists(conn,  k, ts));
            }

            s.removeTable(table1).get();
            s.removeTable(table2).get();
        }
    }

    private boolean exists(AsyncConnection conn, KeyValue.Key key, long ts) {
        var t = conn.getTable(TableName.valueOf(key.table()));
        return t.exists(
                new Get(key.row())
                        .addFamily(key.column())
                        .setTimestamp(ts))
                .join();

    }


    private void checkNoCommittedFlag(AsyncConnection con, String table, KeyValue.Key key, long ts) {
        var t = con.getTable(TableName.valueOf(table));
        var res = t.exists(
                new Get(key.row())
                        .addColumn(key.column(), Constants.UNCOMMITTED_QUALIFIER_BYTES)
                        .setTimestamp(ts))
                .join();
        assertFalse(res);
    }
}
