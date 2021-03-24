import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;

class PutEndpointTest extends EndpointTest {
    private final String row = "row";

    @Test
    void putWithoutConflicts() throws ExecutionException, InterruptedException, IOException {
        try (var conn = ConnectionFactory.createAsyncConnection(conf).get()) {
            deleteTable(conn);
            createTable(conn);

            Map<Long, String> versionToValue = new HashMap<>();
            var random = new Random();

            for (int i = 0; i < 10; i++) {
                var ts = Math.abs(random.nextLong()) % 20;
                if (versionToValue.containsKey(ts)) {
                    continue;
                }

                var val = random.nextDouble() + "";
                versionToValue.put(ts, val);
                expectEndpointPut(conn, row, ts, val, true);
            }

            for (var e : versionToValue.entrySet()) {
                checkUncommittedPut(conn, row, e.getKey(), e.getValue());
            }
        }
    }

    @Test
    void putWithConflicts() throws ExecutionException, InterruptedException, IOException {
        try (var conn = ConnectionFactory.createAsyncConnection(conf).get()) {
            deleteTable(conn);
            createTable(conn);

            // put v1
            expectEndpointPut(conn, row, 1, "v1", true);

            // T2(get, put)
            expectEndpointGet(conn, row, 2, Bytes.toBytes("v1"), 1, false);
            // no conflicts in on txn
            expectEndpointPut(conn, row, 2, "v2", true);

            // read version 2
            expectEndpointGet(conn, row, 5, Bytes.toBytes("v2"), 2, false);

            // try to put version 4
            expectEndpointPut(conn, row, 4, "v4", false);

            // read version 2
            expectEndpointGet(conn, row, 3, Bytes.toBytes("v2"), 2, false);
        }
    }

    @Test
    void putAndGetRandomly() throws ExecutionException, InterruptedException, IOException {
        try (var conn = ConnectionFactory.createAsyncConnection(conf).get()) {
            deleteTable(conn);
            createTable(conn);

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
                        expectEndpointGet(conn, row, ts);
                    } else {
                        var writeTs = opt.get().getKey();
                        var value = opt.get().getValue();

                        expectEndpointGet(conn, row, ts, Bytes.toBytes(value), writeTs, false);
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

                    expectEndpointPut(conn, row, ts, value, !conflict);
                    if (!conflict) versionToValue.put(ts, value);
                }
            }
        }
    }
}
