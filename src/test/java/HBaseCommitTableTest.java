import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;
import org.yangtau.hbs.CommitTable;
import org.yangtau.hbs.HBaseCommitTable;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class HBaseCommitTableTest {
    private final Configuration conf = HBaseConfiguration.create();

    @Test
    void commitTest() throws Exception {
        TestUtil.DeleteTable(conf, HBaseCommitTable.COMMIT_TABLE);
        HBaseCommitTable.createCommitTable(conf);

        var random = new Random();

        try (CommitTable commitTable = new HBaseCommitTable(conf);
             var table = ConnectionFactory.createConnection(conf)
                     .getTable(TableName.valueOf(HBaseCommitTable.COMMIT_TABLE))) {

            for (int i = 0; i < 100; i++) {
                var id = random.nextLong();
                commitTable.commit(id);
                assertTrue(table.exists(new Get(Bytes.toBytes(id))));
                assertTrue(commitTable.exists(id));
            }
        }
    }


    @Test
    void dropTest() throws Exception {
        TestUtil.DeleteTable(conf, HBaseCommitTable.COMMIT_TABLE);
        HBaseCommitTable.createCommitTable(conf);

        var random = new Random();
        Set<Long> txns = new HashSet<>();
        int length = 100;

        try (CommitTable commitTable = new HBaseCommitTable(conf);
             var table = ConnectionFactory.createConnection()
                     .getTable(TableName.valueOf(HBaseCommitTable.COMMIT_TABLE))) {

            for (int i = 0; i < length; i++) {
                var id = random.nextLong();
                if (txns.contains(id)) continue;

                txns.add(id);
                commitTable.commit(id);
                assertTrue(table.exists(new Get(Bytes.toBytes(id))));
            }

            for (var id : txns) {
                assertTrue(commitTable.exists(id));
                commitTable.drop(id);
                assertFalse(table.exists(new Get(Bytes.toBytes(id))));
            }
        }
    }
}
