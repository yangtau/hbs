package org.yangtau.hbs.hbase;

import org.apache.hadoop.hbase.util.Bytes;

final public class Constants {
    // the column "$col:@DATA" is the location of data cell where stores users' data
    public static final String DataQualifier = "@DATA";
    public static final byte[] DataQualifierBytes = Bytes.toBytes(DataQualifier);

    // the column "$col:@RT" indicates whether the newest read timestamp of the data cell
    public static final String ReadTimestampQualifier = "@RT";
    public static final byte[] ReadTimestampQualifierBytes = Bytes.toBytes(ReadTimestampQualifier);

    // the column "$col:@C" indicates whether the data cell has been committed
    // "$col:@C" == "" means that the data cells has not been committed
    // the nonexistence of "$col:@C" means that the data cells has been committed
    public static final String UncommittedQualifier = "@C";
    public static final byte[] UncommittedQualifierBytes = Bytes.toBytes(UncommittedQualifier);
}
