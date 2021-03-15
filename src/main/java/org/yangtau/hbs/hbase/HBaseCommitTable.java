package org.yangtau.hbs.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.yangtau.hbs.CommitTable;

import java.io.IOException;


// implementation of CommitTable in HBase
public class HBaseCommitTable implements CommitTable {
    static public final String COMMIT_TABLE = "HBS_COMMIT_TABLE";
    static public final String COlUMN = "COMMIT";

    private final Table table;
    private final Connection connection;

    public HBaseCommitTable(Configuration conf) throws IOException {
        // TODO: can hbase connection be reused?
        connection = ConnectionFactory.createConnection(conf);
        table = connection.getTable(TableName.valueOf(COMMIT_TABLE));
    }

    // create the commit table in HBase
    // only call this when initialize a system
    static public void createCommitTable(Configuration conf) throws IOException {
        var columnFamilyDes = ColumnFamilyDescriptorBuilder
                .newBuilder(Bytes.toBytes(COlUMN))
                .setMaxVersions(1)
                .build();
        var tableDes = TableDescriptorBuilder
                .newBuilder(TableName.valueOf(COMMIT_TABLE))
                .setColumnFamily(columnFamilyDes)
                .build();

        try (var conn = ConnectionFactory.createConnection();
             var admin = conn.getAdmin()) {
            admin.createTable(tableDes);
        }
    }

    @Override
    public void commit(long id) throws Exception {
        // add to column "$COLUMN:"
        var put = new Put(Bytes.toBytes(id))
                .addColumn(Bytes.toBytes(COlUMN), Bytes.toBytes(""), Bytes.toBytes(""));
        table.put(put);
    }

    @Override
    public boolean exists(long id) throws Exception {
        var get = new Get(Bytes.toBytes(id))
                .addFamily(Bytes.toBytes(COlUMN))
                .setCheckExistenceOnly(true);
        return table.exists(get);
    }

    @Override
    public void drop(long id) throws Exception {
        var delete = new Delete(Bytes.toBytes(id))
                .addFamily(Bytes.toBytes(COlUMN));
        table.delete(delete);
    }

    @Override
    public void close() throws IOException {
        connection.close();
        table.close();
    }
}
