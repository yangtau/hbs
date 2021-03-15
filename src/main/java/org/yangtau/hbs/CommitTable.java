package org.yangtau.hbs;

import java.util.concurrent.CompletableFuture;

// A txn will be written in the CommitTable only if it's first phase successes.
// The point a txn is written in the CommitTable can be viewed as the commit point.
// That is to say, after this point, the mutation of the txn is visible to other txns.
public interface CommitTable {
    CompletableFuture<Void> commit(long id);

    CompletableFuture<Boolean> exists(long id);

    CompletableFuture<Void> drop(long id);
}
