import org.yangtau.hbs.mvto.hbase.coprocessor.GetEndpoint;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;

import java.util.Map;

public class SumTest {
    public static void main(String[] args) throws Throwable {
        Configuration conf = HBaseConfiguration.create();
        try (Connection con = ConnectionFactory.createConnection(conf);
             Admin admin = con.getAdmin()) {
//            TableDescriptor desc = TableDescriptorBuilder.newBuilder(TableName.valueOf("hello"))
//                    .setCoprocessor(GetWithRTCoprocessor.class.getName())
//                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf"))
//                    .build();
//            admin.createTable(desc);

            Map<byte[], Long> res = GetEndpoint.run(conf, "row1");

            for (Long r : res.values()) {
                System.out.println(r);
            }
            if (res.isEmpty()) {
                System.out.println("I got nothing");
            }
        }
    }
}
