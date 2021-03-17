package org.yangtau.hbs;

import java.util.concurrent.CompletableFuture;

public interface CommitTable {
    // check the status of a txn
    CompletableFuture<Transaction.Status> status(long timestamp);

    // commit a txn
    // status: Uncommitted -> Committed
    CompletableFuture<Boolean> commit(long timestamp);

    // abort a txn
    // status: Uncommitted -> Aborted
    CompletableFuture<Boolean> abort(long timestamp);

    // Note:
    // there may be concurrent commit and abort of a txn, only one of them returns true
}
