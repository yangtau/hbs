package org.yangtau.hbs;

import org.apache.hadoop.hbase.util.Bytes;

final public class Constants {
    // the column "$col:@DATA" is the location of data cell where stores users' data
    public static final String DataQualifier = "@DATA";
    public static final byte[] DataQualifierBytes = Bytes.toBytes(DataQualifier);

    // the column "$col:@RT" indicates whether the newest read timestamp of the data cell
    public static final String ReadTimestampQualifier = "@RT";
    public static final byte[] ReadTimestampQualifierBytes = Bytes.toBytes(ReadTimestampQualifier);

    // the column "$col:@C" indicates whether the data cell has been committed
    // "$col:@C" == "" means that the data cells has been committed
    // "$col:@C" == "$ts" means that the data cells has not been committed, and the txn's id is $ts
    public static final String CommitQualifier = "@C";
    public static final byte[] CommitQualifierBytes = Bytes.toBytes(CommitQualifier);

}
