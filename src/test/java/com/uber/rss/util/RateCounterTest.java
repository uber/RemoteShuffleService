package com.uber.rss.util;

import org.testng.Assert;
import org.testng.annotations.Test;

public class RateCounterTest {
    
    @Test
    public void addValueAndGetRate() throws InterruptedException {
        long checkpointMillis = 10;
        long startTime = System.currentTimeMillis();
        
        RateCounter rateCounter = new RateCounter(checkpointMillis);
        
        Double rate = rateCounter.addValueAndGetRate(20);
        if (System.currentTimeMillis() - startTime < checkpointMillis) {
            Assert.assertNull(rate);
        }
        
        Thread.sleep(checkpointMillis);
        rate = rateCounter.addValueAndGetRate(20);
        Assert.assertNotNull(rate);
        Assert.assertTrue(rate > 0);

        Assert.assertTrue(rateCounter.getOverallRate() > 0);
    }

    @Test
    public void checkRateAccuracy() throws InterruptedException {
        long checkpointMillis = 10;

        RateCounter rateCounter = new RateCounter(checkpointMillis);

        Thread.sleep(checkpointMillis);

        Double rate = rateCounter.addValueAndGetRate(100 * checkpointMillis);

        // rate should be roughly between 10 and 1000
        Assert.assertTrue(rate > 10);
        Assert.assertTrue(rate < 1000);
    }
}
