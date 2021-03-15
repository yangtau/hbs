import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;
import org.yangtau.hbs.Storage;
import org.yangtau.hbs.hbase.Constants;
import org.yangtau.hbs.hbase.HBaseStorage;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

class PutEndpointTest extends EndpointTest {
    final String row = "row";

    boolean put(Storage s, long ts, String value) throws ExecutionException, InterruptedException {
        return s.putIfNoConflict(tableName, Bytes.toBytes(row), Bytes.toBytes(family), Bytes.toBytes(value), ts).get();
    }

    void check(AsyncConnection conn, long ts, String value, boolean committed) throws ExecutionException, InterruptedException {
        var res = get(conn, row, ts);
        assertTrue(Arrays.equals(res.getValue(Bytes.toBytes(family), Constants.DataQualifierBytes),
                Bytes.toBytes(value)));

        boolean c = !res.containsColumn(Bytes.toBytes(family), Constants.CommitQualifierBytes);
        assertEquals(committed, c);
    }


    @Test
    void putNoConflicts() throws ExecutionException, InterruptedException, IOException {
        try (var conn = ConnectionFactory.createAsyncConnection(conf).get()) {
            Storage s = new HBaseStorage(conn);
            s.removeTable(tableName).get();
            s.createTable(tableName, List.of(Bytes.toBytes(family)), true).get();

            Map<Long, String> versionToValue = new HashMap<>();
            var random = new Random();

            for (int i = 0; i < 10; i++) {
                var ts = Math.abs(random.nextLong()) % 20;
                if (versionToValue.containsKey(ts)) {
                    continue;
                }

                var val = random.nextDouble() + "";
                versionToValue.put(ts, val);
                assertTrue(put(s, ts, val));
            }

            for (var e : versionToValue.entrySet()) {
                check(conn, e.getKey(), e.getValue(), false);
            }
        }
    }

    @Test
    void putConflict() throws ExecutionException, InterruptedException, IOException {
        try (var conn = ConnectionFactory.createAsyncConnection(conf).get()) {
            Storage s = new HBaseStorage(conn);
            s.removeTable(tableName).get();
            s.createTable(tableName, List.of(Bytes.toBytes(family)), true).get();

            assertTrue(put(s, 1, "v1"));

            // T2(get, put)
            expectGet(s, row, 2, Bytes.toBytes("v1"), 1, false);
            // no conflicts in on txn
            assertTrue(put(s, 2, "v2"));

            // read version 2
            expectGet(s, row, 5, Bytes.toBytes("v2"), 2, false);

            // try to put version 4
            assertFalse(put(s, 4, "v4"));

            // read version 2
            expectGet(s, row, 3, Bytes.toBytes("v2"), 2, false);
        }
    }

    @Test
    void putAndGetRandomly() throws ExecutionException, InterruptedException, IOException {
        try (var conn = ConnectionFactory.createAsyncConnection(conf).get()) {
            Storage s = new HBaseStorage(conn);
            s.removeTable(tableName).get();
            s.createTable(tableName, List.of(Bytes.toBytes(family)), true).get();

            Map<Long, String> versionToValue = new HashMap<>();
            Map<Long, Long> maxRts = new HashMap<>();
            var random = new Random();

            for (int i = 0; i < 20; i++) {
                var ts = Math.abs(random.nextLong()) % 20;
                var opt = versionToValue.entrySet().stream()
                        .filter(e -> e.getKey() < ts)
                        .max((e1, e2) -> (int) (e1.getKey() - e2.getKey()));

                if (random.nextBoolean()) {
                    // get
                    if (opt.isEmpty()) {
                        // nothing can be read, the timestamp is too small
                        expectGetEmpty(s, row, ts);
                    } else {
                        var writeTs = opt.get().getKey();
                        var value = opt.get().getValue();

                        expectGet(s, row, ts, Bytes.toBytes(value), writeTs, false);
                        if (!maxRts.containsKey(writeTs)) {
                            // no read on this version before
                            maxRts.put(writeTs, ts);
                        } else {
                            var preRt = maxRts.get(writeTs);
                            var curRt = preRt > ts ? preRt : ts;
                            maxRts.put(writeTs, curRt);
                        }
                    }
                } else {
                    // put
                    var value = random.nextDouble() + "";
                    var conflict = opt.isPresent() && maxRts.getOrDefault(opt.get().getKey(), -1L) > ts;
                    assertEquals(!conflict, put(s, ts, value), "put timestamp: " + ts);
                    if (!conflict) {
                        versionToValue.put(ts, value);
                    }
                }
            }
        }
    }
}
