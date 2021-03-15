import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.yangtau.hbs.Storage;
import org.yangtau.hbs.hbase.Constants;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

class EndpointTest {
    protected final Configuration conf = HBaseConfiguration.create();
    protected final String tableName = "test";
    protected final String family = "cf";

    protected void put(AsyncConnection con, String row, long ts,
                       String qualifier, String value)
            throws InterruptedException, ExecutionException {
        var table = con.getTable(TableName.valueOf(tableName));
        var put = new Put(Bytes.toBytes(row), ts)
                .addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
        table.put(put).get();
    }

    protected Result get(AsyncConnection con, String row, long ts)
            throws InterruptedException, ExecutionException {
        var table = con.getTable(TableName.valueOf(tableName));
        var get = new Get(Bytes.toBytes(row))
                .addFamily(Bytes.toBytes(family))
                .setTimestamp(ts);
        return table.get(get).get();
    }

    // call endpointGet
    protected void expectGet(Storage s, String row, long ts, byte[] expectValue, long expectVersion, boolean expectCommitted)
            throws ExecutionException, InterruptedException {
        var cell = s.getWithReadTimestamp(tableName, Bytes.toBytes(row), Bytes.toBytes(family), ts).get();
        assertNotNull(cell);
        assertTrue(Arrays.equals(expectValue, cell.value()));
        assertEquals(expectVersion, cell.timestamp());
        assertEquals(expectCommitted, cell.committed());
    }

    // call endpointGet
    protected void expectGetEmpty(Storage s, String row, long ts)
            throws ExecutionException, InterruptedException {
        var cell = s.getWithReadTimestamp(tableName, Bytes.toBytes(row), Bytes.toBytes(family), ts).get();
        assertNull(cell);
    }

    // check read timestamp
    protected void checkReadTimestamp(AsyncConnection conn, String row, long version, long expectRt)
            throws ExecutionException, InterruptedException {
        var res = get(conn, row, version);
        assertTrue(res.containsColumn(Bytes.toBytes(family), Constants.ReadTimestampQualifierBytes));
        assertEquals(expectRt,
                Bytes.toLong(res.getValue(Bytes.toBytes(family), Constants.ReadTimestampQualifierBytes)));
    }

    // check if there is no read timestamp
    protected void checkNoReadTimestamp(AsyncConnection conn, String row, long version)
            throws ExecutionException, InterruptedException {
        var res = get(conn, row, version);
        assertFalse(res.containsColumn(Bytes.toBytes(family), Constants.ReadTimestampQualifierBytes));
    }
}
