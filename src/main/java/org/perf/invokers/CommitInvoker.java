package org.perf.invokers;

import org.HdrHistogram.Histogram;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.jgroups.util.Util;
import org.perf.model.UETRAction;

import jakarta.transaction.HeuristicMixedException;
import jakarta.transaction.HeuristicRollbackException;
import jakarta.transaction.NotSupportedException;
import jakarta.transaction.RollbackException;
import jakarta.transaction.SystemException;
import jakarta.transaction.TransactionManager;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

import static org.perf.IndexingTest.ACTION_CACHE;

public class CommitInvoker implements Invoker {

    private final CountDownLatch latch;
    private final RemoteCacheManager rcm;
    private final Histogram requests = Invoker.createHistogram();

    protected volatile boolean running = true;
    private long numRequests = 0;

    public CommitInvoker(CountDownLatch latch, RemoteCacheManager rcm) {
        this.latch = latch;
        this.rcm = rcm;
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public Histogram getResults() {
        return requests;
    }

    @Override
    public long getCount() {
        return numRequests;
    }

    @Override
    public void run() {
        running = false;
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        RemoteCache<String, UETRAction> actionCache = rcm.getCache(ACTION_CACHE);

        while (running) {
            numRequests++;
            var uuid = UUID.randomUUID().toString();
            var amount = Integer.toString(ThreadLocalRandom.current().nextInt(100));
            long start = Util.micros();
            TransactionManager transactionManager = actionCache.getTransactionManager();
            boolean commit = true;
            try {
                transactionManager.begin();
                actionCache.put(uuid + "_COM", new UETRAction("1234", "4321", uuid, "COM", amount));
            } catch (SystemException | NotSupportedException e) {
                e.printStackTrace();
                commit = false;
            }

            try {
                if (commit) {
                    transactionManager.commit();
                } else {
                    transactionManager.rollback();
                }
            } catch (HeuristicRollbackException | RollbackException | HeuristicMixedException | SystemException e) {
                e.printStackTrace();
            }
            long duration = Util.micros() - start;
            requests.recordValue(duration);
        }
    }
}
