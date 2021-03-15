import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.concurrent.ExecutionException;

final class TestUtil {
    public static void put(AsyncConnection con, String tableName, String row, long ts,
                           String family, String qualifier, String value)
            throws InterruptedException, ExecutionException {
        var table = con.getTable(TableName.valueOf(tableName));
        var put = new Put(Bytes.toBytes(row), ts)
                .addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
        table.put(put).get();
    }

    public static Result get(AsyncConnection con, String tableName, String row, long ts, String family)
            throws InterruptedException, ExecutionException {
        var table = con.getTable(TableName.valueOf(tableName));
        var get = new Get(Bytes.toBytes(row))
                .addFamily(Bytes.toBytes(family))
                .setTimestamp(ts);
        return table.get(get).get();
    }

    public static void DeleteTable(AsyncConnection con, String name)
            throws InterruptedException, ExecutionException {
        TableName tableName = TableName.valueOf(name);

        AsyncAdmin admin = con.getAdmin();

        var list = admin.listTableNames().get();
        for (var t : list) {
            if (t.getNameAsString().equals(name)) {
                // delete 'test'
                admin.disableTable(tableName).get();
                admin.deleteTable(tableName).get();
                break;
            }
        }
    }

    // create a table and a cf who has max version numbers and the largest TTL
    public static void CreateTable(AsyncConnection con, String name, String family)
            throws InterruptedException, ExecutionException {
        TableName tableName = TableName.valueOf(name);
        byte[] familyBytes = Bytes.toBytes(family);

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
