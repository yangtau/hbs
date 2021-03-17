package org.yangtau.hbs;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

public interface TransactionManager extends AutoCloseable {
    // allocate a timestamp, and record it in the TM server (e.g. Zookeeper)
    long allocate() throws Exception;

    // delete the record of the txn in the TM server
    void release(long id) throws Exception;

    // check if a txn exists on the TM server
    boolean exists(long id) throws Exception;

    // wait if a txn exists
    void waitIfExists(long id) throws Exception;

    default CompletableFuture<Long> allocateAsync() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return allocate();
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        });
    }

    default CompletableFuture<Void> releaseAsync(long id) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                release(id);
                return null;
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        });
    }

    default CompletableFuture<Boolean> existsAsync(long id) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return exists(id);
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        });
    }

    default CompletableFuture<Void> waitIfExistsAsync(long id) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                waitIfExists(id);
                return null;
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        });
    }

}
