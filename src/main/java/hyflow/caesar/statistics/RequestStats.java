package hyflow.caesar.statistics;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by balaji on 4/29/16.
 */
public final class RequestStats {

    private static final RequestStats instance = new RequestStats();

    public AtomicInteger fpCount = new AtomicInteger(0);
    public AtomicInteger spCount = new AtomicInteger(0);
    public AtomicInteger retryCount = new AtomicInteger(0);
    public AtomicInteger recoverCount = new AtomicInteger(0);

    private RequestStats() {

    }

    public static RequestStats getInstance() {
        return instance;
    }

    public void printAndResetStats() {
        int fp = fpCount.getAndSet(0);
        int sp = spCount.getAndSet(0);
        int retry = retryCount.getAndSet(0);
        int recover = recoverCount.getAndSet(0);
        System.err.println(String.format("Fast Propose %d, Slow Propose: %d, Retry %d. Recovery %d", fp, sp, retry, recover));
    }

}
