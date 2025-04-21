package com.neeraj.tokenBucket;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class TokenBucketConcurrencyTest {

    private TokenBucket tokenBucket;

    @BeforeEach
    public void setup() {
        tokenBucket = new TokenBucket(20, 10, TimeUnit.SECONDS);
    }

    @Test
    public void testInitialCapacity() {
        assertEquals(20, tokenBucket.getAvailableTokens());
        assertEquals(20, tokenBucket.getCapacity());
    }

    @Test
    public void testBasicConsumption() {
        assertTrue(tokenBucket.tryConsume(1));
        assertEquals(19, tokenBucket.getAvailableTokens());

        // Consume multiple tokens at once
        assertTrue(tokenBucket.tryConsume(5));
        assertEquals(14, tokenBucket.getAvailableTokens());

        // Consume more than what it has
        assertFalse(tokenBucket.tryConsume(16));
        assertEquals(14, tokenBucket.getAvailableTokens());
    }

    @Test
    public void testRefill() throws InterruptedException {
        // Consume all
        for (int i = 0; i < 20; i++) {
            tokenBucket.tryConsume(1);
        }

        assertEquals(0, tokenBucket.getAvailableTokens());

        // Wait for 20 tokens to be refilled, since 10 tokens per second
        TimeUnit.SECONDS.sleep(2);

        tokenBucket.tryConsume(1);

        assertTrue(tokenBucket.getAvailableTokens() >= 18,
                "Expected at least 18 tokens to get refilled but got " + tokenBucket.getAvailableTokens());
    }

    @ParameterizedTest
    @ValueSource(ints = {5, 20, 50, 5000, 10_000})
    public void testConcurrentAccess(int numThreads) throws InterruptedException {
        final int requestPerThread = 10;
        final AtomicInteger allowedRequests = new AtomicInteger(0);
        final AtomicInteger deniedRequests = new AtomicInteger(0);
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch endLatch = new CountDownLatch(numThreads);
        final List<Exception> exceptions = new ArrayList<>();

        @SuppressWarnings("resource")
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        // Start multiple threads that will try to consume tokens concurrently
        for (int i = 0; i < numThreads; i++) {
            executor.submit(() -> {
                try {
                    // Wait for all threads to be ready
                    startLatch.await();

                    for (int j = 0; j < requestPerThread; j++) {
                        if (tokenBucket.tryConsume(1)) {
                            allowedRequests.incrementAndGet();
                        } else {
                            deniedRequests.incrementAndGet();
                        }

                        // Random delay between requests
                        TimeUnit.MILLISECONDS.sleep((long) (Math.random() * 20 + 5));
                    }
                } catch (Exception e) {
                    synchronized (exceptions) {
                        exceptions.add(e);
                    }
                } finally {
                    endLatch.countDown();
                }
            });
        }

        // Start all threads at once
        // Await inside the executorStatement on top is waiting unless
        // all threads are registered with executorService
        // Each worker thread calls startLatch.await(), which makes them pause and wait
        // The main thread calls startLatch.countDown() once, bringing the count to 0
        // Why are doing this to send burst of threads immediately to TokenBucket Rate Limiter
        startLatch.countDown();

        // Wait for all threads to complete
        // Allows the main thread to wait until all worker threads have finished.
        boolean allCompleted = endLatch.await(30, TimeUnit.SECONDS);

        executor.shutdown();
        //noinspection ResultOfMethodCallIgnored
        executor.awaitTermination(5, TimeUnit.SECONDS);

        assertTrue(allCompleted, "Not all threads completed in time");
        assertTrue(exceptions.isEmpty(), "Exception occurred during test" + exceptions);

        int totalRequests = allowedRequests.get() + deniedRequests.get();
        assertEquals(numThreads * requestPerThread, totalRequests,
                "Total Processes request should match expected count");

        // Log the results for analysis
        System.out.printf("Test with %d threads: Allowed=%d, Denied=%d, Total=%d%n",
                numThreads, allowedRequests.get(), deniedRequests.get(), totalRequests);

        if (numThreads > 20) {
            assertTrue(deniedRequests.get() > 0, "High concurrency should cause some requests to be denied");
        }
    }


}
