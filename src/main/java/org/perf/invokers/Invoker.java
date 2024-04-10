package org.perf.invokers;

import org.HdrHistogram.Histogram;

public interface Invoker extends Runnable {

    void cancel();

    Histogram getResults();

    long getCount();

    static Histogram createHistogram() {
        // max recordable value is 80s
        return new Histogram(1, 80_000_000, 3); // us
    }

}
