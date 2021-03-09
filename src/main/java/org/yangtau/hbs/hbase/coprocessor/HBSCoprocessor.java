package org.yangtau.hbs.hbase.coprocessor;

import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.regionserver.Region;

import java.io.IOException;

interface HBSCoprocessor extends RegionCoprocessor {
    default Region.RowLock getLock(Region region, byte[] row) throws IOException {
        region.startRegionOperation();
        try {
            // exclusive lock
            return region.getRowLock(row, false);
        } catch (IOException e) {
            // make sure that closeRegionOperation is called if getRowLock throws an exception
            region.closeRegionOperation();
            throw e;
        }
    }

    default void releaseLock(Region region, Region.RowLock lock) throws IOException {
        lock.release(); // releasing locks does not cause exception
        region.closeRegionOperation();
    }

}
