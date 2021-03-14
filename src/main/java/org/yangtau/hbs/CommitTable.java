package org.yangtau.hbs;

import java.io.Closeable;

// A txn will be written in the CommitTable only if it's first phase successes.
// The point a txn is written in the CommitTable can be viewed as the commit point.
// That is to say, after this point, the mutation of the txn is visible to other txns.
public interface CommitTable extends Closeable {
    void commit(long id) throws Exception;

    boolean exists(long id) throws Exception;

    void drop(long id) throws Exception;
}
