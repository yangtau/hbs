import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

final class TestUtil {
    public static void put(Configuration conf, String tableName, String row, long ts, String family, String qualifier,
                           String value) throws InterruptedException, ExecutionException, IOException {
        try (AsyncConnection con = ConnectionFactory.createAsyncConnection(conf).get()) {
            var table = con.getTable(TableName.valueOf(tableName));
            var put = new Put(Bytes.toBytes(row), ts)
                    .addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            table.put(put).get();
        }
    }

    public static Result get(Configuration conf, String tableName, String row, long ts, String family)
            throws IOException, InterruptedException, ExecutionException {
        try (AsyncConnection con = ConnectionFactory.createAsyncConnection(conf).get()) {
            var table = con.getTable(TableName.valueOf(tableName));
            var get = new Get(Bytes.toBytes(row))
                    .addFamily(Bytes.toBytes(family))
                    .setTimestamp(ts);
            return table.get(get).get();
        }
    }

    public static void DeleteTable(Configuration conf, String name, String family)
            throws InterruptedException, ExecutionException, IOException {
        TableName tableName = TableName.valueOf(name);
        byte[] familyBytes = Bytes.toBytes(family);

        try (AsyncConnection con = ConnectionFactory.createAsyncConnection(conf).get()) {
            AsyncAdmin admin = con.getAdmin();

            // delete 'test'
            admin.disableTable(tableName).get();
            admin.deleteTable(tableName).get();
        }
    }

    // create a table and a cf who has max version numbers and the largest TTL
    public static void CreateTable(Configuration conf, String name, String family)
            throws InterruptedException, ExecutionException, IOException {
        TableName tableName = TableName.valueOf(name);
        byte[] familyBytes = Bytes.toBytes(family);

        try (AsyncConnection con = ConnectionFactory.createAsyncConnection(conf).get()) {
            AsyncAdmin admin = con.getAdmin();
            var columnFamilyDes = ColumnFamilyDescriptorBuilder.newBuilder(familyBytes)
                    .setMaxVersions(Integer.MAX_VALUE)
                    .setTimeToLive(Integer.MAX_VALUE)
                    .build();
            var tableDes = TableDescriptorBuilder.newBuilder(tableName)
                    .setColumnFamily(columnFamilyDes)
                    .build();

            // create 'test'
            admin.createTable(tableDes).get();
        }
    }
}
