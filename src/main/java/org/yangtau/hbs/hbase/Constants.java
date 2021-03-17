package org.yangtau.hbs.hbase;

import org.apache.hadoop.hbase.util.Bytes;

final public class Constants {
    // the column "$column:@DATA" is the location of data cell where stores users' data
    public static final String DATA_QUALIFIER = "@DATA";
    public static final byte[] DATA_QUALIFIER_BYTES = Bytes.toBytes(DATA_QUALIFIER);

    // the column "$column:@RT" indicates whether the newest read timestamp of the data cell
    public static final String READ_TIMESTAMP_QUALIFIER = "@RT";
    public static final byte[] READ_TIMESTAMP_QUALIFIER_BYTES = Bytes.toBytes(READ_TIMESTAMP_QUALIFIER);

    // the column "$column:@C" indicates whether the data cell has been committed
    // "$column:@C" == "" means that the data cells has not been committed
    // the nonexistence of "$column:@C" means that the data cells has been committed
    public static final String UNCOMMITTED_QUALIFIER = "@C";
    public static final byte[] UNCOMMITTED_QUALIFIER_BYTES = Bytes.toBytes(UNCOMMITTED_QUALIFIER);
}
