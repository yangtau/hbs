import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;
import org.yangtau.hbs.Storage;
import org.yangtau.hbs.hbase.Constants;
import org.yangtau.hbs.hbase.HBaseStorage;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;


class GetEndpointTest extends EndpointTest {
    @Test
    void getCommittedAndUncommitted() throws ExecutionException, InterruptedException, IOException {
        try (var conn = ConnectionFactory.createAsyncConnection(conf).get()) {
            Storage s = new HBaseStorage(conn);
            s.removeTable(tableName).get();
            s.createTable(tableName, List.of(Bytes.toBytes(family)), true).get();

            var value = "hello";

            // Read uncommitted
            // get with a small timestamp, no RT should be written
            put(conn, "row1", 1L, Constants.DataQualifier, value);
            put(conn, "row1", 1L, Constants.UncommittedQualifier, "");
            checkNoReadTimestamp(conn, "row1", 1L);
            // nothing should be read
            expectGetEmpty(s, "row1", 1L);
            checkNoReadTimestamp(conn, "row1", 1L);
            // expect write a (RT: 2L) in version 1L
            expectGet(s, "row1", 2L, Bytes.toBytes(value), 1L, false);
            checkReadTimestamp(conn, "row1", 1L, 2L);

            // Read committed
            put(conn, "row1", 3L, Constants.DataQualifier, value);
            checkNoReadTimestamp(conn, "row1", 3L);
            // expect write a (RT: 4L) in version 3L
            expectGet(s, "row1", 4L, Bytes.toBytes(value), 3L, true);
            checkReadTimestamp(conn, "row1", 3L, 4L);
        }
    }

    @Test
    void getFromMultiVersions() throws ExecutionException, InterruptedException, IOException {
        try (var conn = ConnectionFactory.createAsyncConnection(conf).get()) {
            Storage s = new HBaseStorage(conn);
            s.removeTable(tableName).get();
            s.createTable(tableName, List.of(Bytes.toBytes(family)), true).get();

            String row = "row100";
            long length = 20;
            Map<Long, byte[]> versionToValue = new HashMap<>();
            var random = new Random();

            for (long i = 0L; i < length; i += 3L) {
                var ts = Math.abs(random.nextLong()) % length;
                var value = random.nextDouble() + "";
                put(conn, row, ts, Constants.DataQualifier, value);
                versionToValue.put(ts, Bytes.toBytes(value));
            }


            for (long i = 0L; i < length + 1L; i++) {
                final var ts = i;
                var opt = versionToValue.entrySet().stream()
                        .filter(e -> e.getKey() < ts)
                        .max((e1, e2) -> (int) (e1.getKey() - e2.getKey()));

                if (opt.isEmpty()) {
                    expectGetEmpty(s, row, ts);
                } else {
                    expectGet(s, row, ts, opt.get().getValue(), opt.get().getKey(), true);
                }
            }
        }
    }

    @Test
    void multiGetOnTheSameData() throws ExecutionException, InterruptedException, IOException {
        try (var conn = ConnectionFactory.createAsyncConnection(conf).get()) {
            Storage s = new HBaseStorage(conn);
            s.removeTable(tableName).get();
            s.createTable(tableName, List.of(Bytes.toBytes(family)), true).get();

            // version 20
            String row = "favorite programming language";
            String value20 = "hedgehog";
            long writeTs20 = 20L;
            put(conn, row, writeTs20, Constants.DataQualifier, value20);

            // version 15
            String value15 = "lisp";
            long writeTs15 = 15L;
            put(conn, row, writeTs15, Constants.DataQualifier, value15);

            checkNoReadTimestamp(conn, row, writeTs15);
            checkNoReadTimestamp(conn, row, writeTs20);

            // case 1: no RT before
            {
                // get version 15
                expectGet(s, row, 20, Bytes.toBytes(value15), 15, true);
                checkReadTimestamp(conn, row, 15, 20);
                // get version 20
                expectGet(s, row, 30, Bytes.toBytes(value20), 20, true);
                checkReadTimestamp(conn, row, 20, 30);
            }
            // case 2: smaller RT than current one
            // WT  RT
            // 20  30
            // 15  20
            {
                // get version 15
                expectGet(s, row, 19, Bytes.toBytes(value15), 15, true);
                checkReadTimestamp(conn, row, 15, 20);
                // get version 20
                expectGet(s, row, 21, Bytes.toBytes(value20), 20, true);
                checkReadTimestamp(conn, row, 20, 30);
            }
            // case 3: bigger RT than current one
            // WT  RT
            // 20  30
            // 15  20
            {
                // get version 20
                expectGet(s, row, 31, Bytes.toBytes(value20), 20, true);
                checkReadTimestamp(conn, row, 20, 31);
            }
        }
    }

    @Test
    void randomGet() throws ExecutionException, InterruptedException, IOException {
        try (var conn = ConnectionFactory.createAsyncConnection(conf).get()) {
            Storage s = new HBaseStorage(conn);
            s.removeTable(tableName).get();
            s.createTable(tableName, List.of(Bytes.toBytes(family)), true).get();


            String row = "row100";
            long maxTs = 20;
            Map<Long, byte[]> versionToValue = new HashMap<>();
            Map<Long, Long> maxRts = new HashMap<>();
            var random = new Random();

            // prepared data in the `row`
            for (long i = 0L; i < maxTs; i += 3L) {
                var ts = Math.abs(random.nextLong()) % maxTs;
                var value = random.nextDouble() + "";
                put(conn, row, ts, Constants.DataQualifier, value);
                versionToValue.put(ts, Bytes.toBytes(value));
            }


            for (long i = 0L; i < maxTs * 2; i++) {
                var ts = Math.abs(random.nextLong()) % (maxTs * 2);

                // the version will be read
                var opt = versionToValue.entrySet().stream()
                        .filter(e -> e.getKey() < ts)
                        .max((e1, e2) -> (int) (e1.getKey() - e2.getKey()));
                if (opt.isEmpty()) {
                    // nothing can be read, the timestamp is too small
                    expectGetEmpty(s, row, ts);
                } else {
                    var writeTs = opt.get().getKey();
                    var value = opt.get().getValue();

                    // check RT before Get
                    if (!maxRts.containsKey(writeTs)) {
                        // no read on this version before
                        checkNoReadTimestamp(conn, row, writeTs);
                    }

                    expectGet(s, row, ts, value, writeTs, true);
                    if (!maxRts.containsKey(writeTs)) {
                        // no read on this version before
                        maxRts.put(writeTs, ts);
                        checkReadTimestamp(conn, row, writeTs, ts);
                    } else {
                        var preRt = maxRts.get(writeTs);
                        var curRt = preRt > ts ? preRt : ts;
                        maxRts.put(writeTs, curRt);
                        checkReadTimestamp(conn, row, writeTs, curRt);
                    }
                }
            }
        }
    }

}
