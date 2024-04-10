package org.perf.invokers;

import org.HdrHistogram.Histogram;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.jgroups.util.Util;
import org.perf.model.Account;
import org.perf.model.UETRAction;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static org.perf.IndexingTest.ACCOUNT_CACHE;
import static org.perf.IndexingTest.ACTION_CACHE;

public class ReserveInvoker implements Invoker {
    private final CountDownLatch latch;
    private final RemoteCacheManager rcm;
    private final Histogram requests = Invoker.createHistogram();
    private final Histogram query = Invoker.createHistogram();

    protected volatile boolean running = true;
    private long numRequests = 0;

    public ReserveInvoker(CountDownLatch latch, RemoteCacheManager rcm) {
        this.latch = latch;
        this.rcm = rcm;
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public void run() {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        RemoteCache<String, Account> accountCache = rcm.getCache(ACCOUNT_CACHE);
        RemoteCache<String, UETRAction> actionCache = rcm.getCache(ACTION_CACHE);
        var queryFactory = Search.getQueryFactory(actionCache);

        while (running) {
            numRequests++;
            String uuid = UUID.randomUUID().toString();
            long start = Util.micros();
            Account acc = accountCache.get("1234");

            long start2 = Util.micros();
            var amount = fetchAccountBalance(acc.accountId, queryFactory);
            //var amount = fetchAccountBalanceIterating(acc.accountId, actionCache);
            query.recordValue(Util.micros() - start2);

            UETRAction action = new UETRAction(acc.accountId, "", uuid, "RES", amount.toPlainString());

            actionCache.put(uuid + "_RES", action);
            requests.recordValue(Util.micros() - start);
        }
    }

    @Override
    public long getCount() {
        return numRequests;
    }

    @Override
    public Histogram getResults() {
        return requests;
    }

    public Histogram getQuery() {
        return query;
    }

    private BigDecimal fetchAccountBalance(String accountId, QueryFactory queryFactory) {
        Query<Object[]> query = queryFactory.create(
                "select fromAccountId, toAccountId, action, amount FROM uetraction.UETRAction WHERE fromAccountId = :accountId or toAccountId = :accountId");
        query.setParameter("accountId", accountId);
        List<Object[]> list = query.maxResults(1000).hitCountAccuracy(10).list();

        return list.stream()
                .map(action -> new BigDecimal((String) action[3]))
                .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    private BigDecimal fetchAccountBalanceIterating(String accountId, RemoteCache<String, UETRAction> cache) {
        var amount = BigDecimal.ZERO;
        try (var iterator = cache.retrieveEntries(null, 256)) {
            while (iterator.hasNext()) {
                UETRAction action = (UETRAction) iterator.next().getValue();
                if (accountId.equals(action.getToAccountId()) || accountId.equals(action.getFromAccountId())) {
                    amount = amount.add(new BigDecimal(action.getAmount()));
                }
            }
        }
        return amount;
    }
}
