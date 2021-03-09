package org.yangtau.hbs.hbase.coprocessor;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ServiceCaller;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;
import org.yangtau.hbs.Constants;
import org.yangtau.hbs.hbase.autogenerated.PutProtos;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class PutEndpoint extends PutProtos.PutService implements HBSCoprocessor {
    private RegionCoprocessorEnvironment env;

    // run Put request on the coprocessor
    public static CompletableFuture<PutProtos.PutResponse> runAsync(
            final Configuration conf, String table, PutProtos.PutRequest request) {
        ServiceCaller<Stub, PutProtos.PutResponse> put =
                (stub, controller, callback) -> stub.put(controller, request, callback);
        return ConnectionFactory.createAsyncConnection(conf)
                .thenApplyAsync(con -> con.getTable(TableName.valueOf(table)))
                .thenComposeAsync(t ->
                        t.coprocessorService(PutProtos.PutService::newStub, put, request.getRow().toByteArray())
                );
    }

    public static PutProtos.PutResponse run(final Configuration conf, String table, PutProtos.PutRequest request)
            throws ExecutionException, InterruptedException {
        return runAsync(conf, table, request).get();
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        if (env instanceof RegionCoprocessorEnvironment) this.env = (RegionCoprocessorEnvironment) env;
        else throw new CoprocessorException("Must be loaded on a table region!");
    }

    @Override
    public Iterable<Service> getServices() {
        return Collections.singleton(this);
    }

    /***************************************************************************
     * Algorithm of Put:
     * data mode: key -> (value, WT, RT, C)
     * Put(key, value, TS):
     *     version =  max(c.WT if c.WT < TS for c in data[key])
     *     cell = for c in data[key] where c.WT == version
     *     if cell.RT > TS:
     *          abort
     *     data[key] = (value, TS, 0, false)
     */
    @Override
    public void put(RpcController controller, PutProtos.PutRequest request, RpcCallback<PutProtos.PutResponse> done) {
        Region region = env.getRegion();
        PutProtos.PutResponse.Builder rsp = PutProtos.PutResponse.newBuilder().setError(false).setResult(false);
        byte[] row = request.getRow().toByteArray();
        byte[] family = request.getColumn().toByteArray();
        byte[] value = request.getValue().toByteArray();
        long requestTs = request.getTimestamp();

        // the RT of the data of the last version in the row
        long readTs = -1;

        try {
            Get get = new Get(row)
                    .setTimeRange(0, requestTs)
                    .addColumn(family, Constants.ReadTimestampQualifierBytes)
                    .readVersions(1);
            Put put = new Put(row, requestTs)
                    .addColumn(family, Constants.DataQualifierBytes, value)
                    .addColumn(family, Constants.CommitQualifierBytes, Bytes.toBytes(""));

            Region.RowLock lock = getLock(region, row);
            try {
                // read the last version
                for (Cell c : region.get(get, false)) {
                    byte[] qualifier = c.getQualifierArray();
                    if (Arrays.equals(qualifier, Constants.ReadTimestampQualifierBytes)) {
                        readTs = Bytes.toLong(c.getValueArray());
                    }
                }

                if (readTs < requestTs) {
                    // no conflicts
                    region.put(put);
                    rsp.setResult(true);
                }
            } finally {
                releaseLock(region, lock);
            }

            done.run(rsp.build());
        } catch (IOException e) {
            done.run(rsp
                    .setError(true)
                    .setErrorMsg(e.toString())
                    .build());
        }
    }
}
