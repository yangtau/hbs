import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.yangtau.hbs.hbase.Constants;
import org.yangtau.hbs.hbase.autogenerated.GetProtos;
import org.yangtau.hbs.hbase.autogenerated.PutProtos;
import org.yangtau.hbs.hbase.coprocessor.GetEndpoint;
import org.yangtau.hbs.hbase.coprocessor.PutEndpoint;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

class EndpointTest {
    protected final Configuration conf = HBaseConfiguration.create();
    protected final String tableName = "TEST";
    protected final String family = "CF";

    protected void createTable(AsyncConnection conn) throws ExecutionException, InterruptedException {
        var columnFamilyDescriptor = ColumnFamilyDescriptorBuilder
                .newBuilder(Bytes.toBytes(family))
                .setMaxVersions(Integer.MAX_VALUE)
                .setTimeToLive(Integer.MAX_VALUE)
                .build();
        var tableDescriptor = TableDescriptorBuilder
                .newBuilder(TableName.valueOf(tableName))
                .setColumnFamily(columnFamilyDescriptor)
                .build();
        conn.getAdmin().createTable(tableDescriptor).get();
    }

    protected void deleteTable(AsyncConnection conn) throws ExecutionException, InterruptedException {
        var admin = conn.getAdmin();
        admin.listTableNames().thenComposeAsync(
                (list) -> {
                    for (var t : list)
                        if (t.getNameAsString().equals(tableName))
                            return admin.disableTable(t).thenComposeAsync((v) -> admin.deleteTable(t));
                    return CompletableFuture.runAsync(() -> {
                    });
                }
        ).get();
    }

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
    protected void expectEndpointGet(AsyncConnection conn, String row, long ts,
                                     byte[] expectValue, long expectVersion, boolean expectCommitted)
            throws ExecutionException, InterruptedException {
        var rsp = GetEndpoint.runAsync(conn, tableName, Bytes.toBytes(row),
                GetProtos.GetRequest.newBuilder()
                        .setTimestamp(ts)
                        .setColumn(ByteString.copyFromUtf8(family))
                        .setRow(ByteString.copyFromUtf8(row))
                        .build()).get();

        assertFalse(rsp.getError(), rsp.getErrorMsg());
        assertTrue(Arrays.equals(expectValue, rsp.getValue().toByteArray()));
        assertEquals(expectVersion, rsp.getTimestamp());
        assertEquals(expectCommitted, rsp.getCommitted());
    }

    // call endpointGet
    protected void expectEndpointGet(AsyncConnection conn, String row, long ts)
            throws ExecutionException, InterruptedException {
        var rsp = GetEndpoint.runAsync(conn, tableName, Bytes.toBytes(row),
                GetProtos.GetRequest.newBuilder()
                        .setTimestamp(ts)
                        .setColumn(ByteString.copyFromUtf8(family))
                        .setRow(ByteString.copyFromUtf8(row))
                        .build()).get();

        assertFalse(rsp.getError(), rsp.getErrorMsg());
        assertFalse(rsp.hasValue());
    }

    protected void expectEndpointPut(AsyncConnection con, String row, long ts, String value, boolean expectedResult)
            throws ExecutionException, InterruptedException {
        var rsp = PutEndpoint.runAsync(con, tableName, Bytes.toBytes(row),
                PutProtos.PutRequest.newBuilder()
                        .setTimestamp(ts)
                        .setRow(ByteString.copyFromUtf8(row))
                        .setColumn(ByteString.copyFromUtf8(family))
                        .setValue(ByteString.copyFromUtf8(value))
                        .build()).get();
        assertFalse(rsp.getError(), rsp.getErrorMsg());
        assertEquals(expectedResult, rsp.getResult());
    }

    // check result of EndpointPut
    void checkUncommittedPut(AsyncConnection conn, String row, long ts, String value) throws ExecutionException, InterruptedException {
        var res = get(conn, row, ts);
        assertTrue(Arrays.equals(res.getValue(Bytes.toBytes(family), Constants.DATA_QUALIFIER_BYTES),
                Bytes.toBytes(value)));

        assertTrue(res.containsColumn(Bytes.toBytes(family), Constants.UNCOMMITTED_QUALIFIER_BYTES));
    }


    // check read timestamp
    protected void checkReadTimestamp(AsyncConnection conn, String row, long version, long expectRt)
            throws ExecutionException, InterruptedException {
        var res = get(conn, row, version);
        assertTrue(res.containsColumn(Bytes.toBytes(family), Constants.READ_TIMESTAMP_QUALIFIER_BYTES));
        assertEquals(expectRt,
                Bytes.toLong(res.getValue(Bytes.toBytes(family), Constants.READ_TIMESTAMP_QUALIFIER_BYTES)));
    }

    // check if there is no read timestamp
    protected void checkReadTimestamp(AsyncConnection conn, String row, long version)
            throws ExecutionException, InterruptedException {
        var res = get(conn, row, version);
        assertFalse(res.containsColumn(Bytes.toBytes(family), Constants.READ_TIMESTAMP_QUALIFIER_BYTES));
    }
}
