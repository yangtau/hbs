import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;
import org.yangtau.hbs.CommitTable;
import org.yangtau.hbs.hbase.HBaseCommitTable;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HBaseCommitTableTest {
    private final Configuration conf = HBaseConfiguration.create();

    @Test
    void commitTest() throws Exception {
        try (AsyncConnection conn = ConnectionFactory.createAsyncConnection(conf).get()) {
            TestUtil.DeleteTable(conn, HBaseCommitTable.COMMIT_TABLE);
            HBaseCommitTable.createCommitTable(conn).get();

            var random = new Random();

            CommitTable commitTable = new HBaseCommitTable(conn);
            var table = conn.getTable(TableName.valueOf(HBaseCommitTable.COMMIT_TABLE));

            for (int i = 0; i < 100; i++) {
                var id = random.nextLong();
                commitTable.commit(id).get();
                assertTrue(table.exists(new Get(Bytes.toBytes(id))).get());
                assertTrue(commitTable.exists(id).get());
            }
        }
    }


    @Test
    void dropTest() throws Exception {
        try (AsyncConnection conn = ConnectionFactory.createAsyncConnection(conf).get()) {
            TestUtil.DeleteTable(conn, HBaseCommitTable.COMMIT_TABLE);
            HBaseCommitTable.createCommitTable(conn).get();

            var random = new Random();
            Set<Long> txns = new HashSet<>();
            int length = 100;

            CommitTable commitTable = new HBaseCommitTable(conn);
            var table = conn.getTable(TableName.valueOf(HBaseCommitTable.COMMIT_TABLE));

            for (int i = 0; i < length; i++) {
                var id = random.nextLong();
                if (txns.contains(id)) continue;

                txns.add(id);
                commitTable.commit(id).get();
                assertTrue(table.exists(new Get(Bytes.toBytes(id))).get());
            }

            for (var id : txns) {
                assertTrue(commitTable.exists(id).get());
                commitTable.drop(id).get();
                assertFalse(table.exists(new Get(Bytes.toBytes(id))).get());
            }
        }
    }
}
