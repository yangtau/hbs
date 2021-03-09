import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.yangtau.hbs.Constants;
import org.yangtau.hbs.hbase.autogenerated.GetProtos;
import org.yangtau.hbs.hbase.coprocessor.GetEndpoint;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class GetEndpointTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        TableName tableName = TableName.valueOf("test");
        byte[] family = Bytes.toBytes("cf");

        try (AsyncConnection con = ConnectionFactory
                .createAsyncConnection(conf)
                .get()) {
            AsyncAdmin admin = con.getAdmin();

            // delete 'test'
            admin.disableTable(tableName).get();
            admin.deleteTable(tableName).get();

            // create 'test'
            admin.createTable(TableDescriptorBuilder.newBuilder(tableName)
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(family).build())
                    .build()).get();

            AsyncTable<AdvancedScanResultConsumer> table = con.getTable(tableName);

            byte[] row = Bytes.toBytes("row3");
            Put put = new Put(row, 2L)
                    .addColumn(family, Constants.DataQualifierBytes, Bytes.toBytes("hello"));
            table.put(put).get();

            GetProtos.GetRequest request = GetProtos.GetRequest.newBuilder()
                    .setRow(ByteString.copyFrom(row))
                    .setColumn(ByteString.copyFrom(family))
                    .setTimestamp(3L)
                    .build();
            GetProtos.GetResponse rsp = GetEndpoint.run(conf, "test", request);

            if (rsp.getError()) {
                System.out.println(rsp.getErrorMsg());
            } else {
                System.out.println(rsp);
            }

        } catch (IOException e) {
            System.out.println("failed");
        }
    }
}