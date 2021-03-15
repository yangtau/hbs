package org.yangtau.hbs.hbase.coprocessor;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;
import org.yangtau.hbs.hbase.autogenerated.GetProtos;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

public class GetEndpoint extends GetProtos.GetService implements HBSCoprocessor {
    private RegionCoprocessorEnvironment env;

    static public CompletableFuture<GetProtos.GetResponse> runAsync(
            AsyncTable<AdvancedScanResultConsumer> asyncTable, GetProtos.GetRequest request) {
        ServiceCaller<Stub, GetProtos.GetResponse> get =
                (stub, controller, callback) -> stub.get(controller, request, callback);
        return asyncTable.coprocessorService(GetProtos.GetService::newStub, get, request.getRow().toByteArray());
    }

    // run Get request on the coprocessor
    static public CompletableFuture<GetProtos.GetResponse> runAsync(
            AsyncConnection conn, String table, GetProtos.GetRequest request) {
        var asyncTable = conn.getTable(TableName.valueOf(table));
        return runAsync(asyncTable, request);
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        if (env instanceof RegionCoprocessorEnvironment)
            this.env = (RegionCoprocessorEnvironment) env;
        else
            throw new CoprocessorException("Must be loaded on a table region!");
    }

    @Override
    public Iterable<Service> getServices() {
        return Collections.singleton(this);
    }


    /***************************************************************************
     * Algorithm of Get:
     * data mode: key -> (value, WT, RT, C)
     * get(key, TS):
     *     version =  max(c.WT if c.WT < TS for c in data[key])
     *     cell = for c in data[key] where c.WT == version
     *     if cell.RT < TS:
     *         cell.RT = TS
     *     return cell
     */
    @Override
    public void get(RpcController controller, GetProtos.GetRequest request, RpcCallback<GetProtos.GetResponse> done) {
        Region region = env.getRegion();
        byte[] row = request.getRow().toByteArray();
        byte[] family = request.getColumn().toByteArray();
        long requestTs = request.getTimestamp();

        var rspBuilder = GetProtos.GetResponse.newBuilder().setError(false);

        long writeTs = -1; // WT for the data cell to be read (version(timestamp) of the data cell)
        long readTs = -1; // RT for the data cell
        byte[] data = null; // data cell value
        boolean committed = true; // commit cell value

        try {
            Get get = new Get(row)
                    .addFamily(family)
                    .setTimeRange(0, requestTs) // read in [0, requestTs)
                    .readVersions(1); // read the last version

            Region.RowLock lock = getLock(region, row);
            try {
                var res = region.get(get);

                // Data
                var dataCell = res.getColumnLatestCell(family, Constants.DataQualifierBytes);
                if (dataCell != null) {
                    data = dataCell.getValueArray();
                    writeTs = dataCell.getTimestamp();

                    // Read @C & @RT only if there is data cell
                    // Committed Flag
                    var commitCell = res.getColumnLatestCell(family, Constants.CommitQualifierBytes);
                    if (commitCell != null && commitCell.getTimestamp() == writeTs)
                        committed = false;

                    // RT
                    var rtCell = res.getColumnLatestCell(family, Constants.ReadTimestampQualifierBytes);
                    if (rtCell != null && rtCell.getTimestamp() == writeTs)
                        readTs = Bytes.toLong(rtCell.getValueArray());
                }

                // this GET has greater Ts than what has been recorded (readTs)
                // put(col: "$family:@RT", value: Ts, version: writeTs)
                if (readTs < requestTs && data != null)
                    region.put(new Put(row, writeTs)
                            .addColumn(family, Constants.ReadTimestampQualifierBytes, Bytes.toBytes(requestTs)));
            } finally {
                releaseLock(region, lock);
            }

            // callback after releasing locks
            if (data != null)
                rspBuilder.setCommitted(committed).setTimestamp(writeTs).setValue(ByteString.copyFrom(data));
            done.run(rspBuilder.build());
        } catch (IOException e) {
            done.run(rspBuilder
                    .setError(true)
                    .setErrorMsg(e.toString())
                    .build());
        }
    }
}
