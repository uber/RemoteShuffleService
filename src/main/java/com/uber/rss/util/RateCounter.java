package com.uber.rss.util;

import java.util.concurrent.atomic.AtomicLong;

/***
 * This class wraps the logic to track a counter and its value change rate.
 */
public class RateCounter {
    private final AtomicLong totalValue = new AtomicLong();
    private final AtomicLong checkpointValue = new AtomicLong();
    private final AtomicLong checkpointTime = new AtomicLong();
    
    private final long checkpointMillis;
    
    private final long overallStartTime;

    /***
     * Creates an instance.
     * @param checkpointMillis this is the checkpoint interval. When time elapses longer than
     *                         this interval, we will calculate the value change rate.
     */
    public RateCounter(long checkpointMillis) {
        if (checkpointMillis <= 0) {
            throw new RuntimeException("Invalid value for checkpointMillis: " + checkpointMillis);
        }
        this.checkpointMillis = checkpointMillis;
        
        this.overallStartTime = System.currentTimeMillis();
        this.checkpointTime.set(this.overallStartTime);
    }

    /***
     * Add value, and return the rate if time has elapsed longer than checkpoint interval.
     * @param delta
     * @return
     */
    public Double addValueAndGetRate(long delta) {
        long newValue = totalValue.addAndGet(delta);
        
        long currentTime = System.currentTimeMillis();
        long lastCheckpointTime = checkpointTime.get();
        if (currentTime - lastCheckpointTime >= checkpointMillis) {
            checkpointTime.set(currentTime);
            long prevCheckpointValue = checkpointValue.getAndSet(newValue);
            double ratePerMillis = (double)(newValue - prevCheckpointValue) / (double)(currentTime - lastCheckpointTime);
            return ratePerMillis;
        }
        
        return null;
    }

    /***
     * Get overall value.
     * @return
     */
    public long getOverallValue() {
        return totalValue.get();
    }
    
    /***
     * Get overall change rate since this instance is created.
     * @return
     */
    public double getOverallRate() {
        long duration = System.currentTimeMillis() - overallStartTime;
        if (duration == 0) {
            return 0.0;
        }

        double ratePerMillis = (double)totalValue.get() / (double)duration;
        return ratePerMillis;
    }
}
