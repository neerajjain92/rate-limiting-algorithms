package com.neeraj.tokenBucket;

import lombok.Getter;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A thread-safe implementation of the Token Bucket algorithm for rate limiting.
 * <p>
 * This implementation focuses on local (in-process) rate limiting.
 */
public class TokenBucket {

    @Getter
    private final long capacity;            // Max tokens the bucket can hold
    private final double refillRatePerNano; // Tokens added per nanosecond

    private final AtomicLong availableTokens; // Current number of tokens
    private final AtomicLong lastRefillTimestampNano; // Last refill time in nanos

    // Using a lock for the critical section of consuming tokens to ensure atomicity
    // of the read-modify-write operation involving both tokens and timestamp.
    private final ReentrantLock lock = new ReentrantLock();


    /**
     * Creates a Token Bucket.
     *
     * @param capacity   Max tokens the bucket can hold. Must be > 0.
     * @param refillRate Tokens added per unit of time. Must be > 0.
     * @param refillUnit Time unit for the refillRate.
     */
    public TokenBucket(long capacity, long refillRate, TimeUnit refillUnit) {
        if (capacity <= 0 || refillRate <= 0) {
            throw new IllegalArgumentException("Capacity and refill rate must be positive");
        }
        this.capacity = capacity;
        // Calculate refill rate per nanosecond for precise calculations
        long refillPeriodNanos = refillUnit.toNanos(1);
        this.refillRatePerNano = (double) refillRate / refillPeriodNanos;

        // Start with the full capacity of our bucket
        this.availableTokens = new AtomicLong(capacity);
        this.lastRefillTimestampNano = new AtomicLong(System.nanoTime());
    }

    public static void main(String[] args) throws InterruptedException {
        // Example: Allow 10 requests per second, with a burst capacity of 20
        TokenBucket bucket = new TokenBucket(20, 10, TimeUnit.SECONDS);

        // Simulate initial burst
        System.out.println("--- Initial Burst ---");
        for (int i = 0; i < 25; i++) {
            System.out.println("Request " + (i + 1) + ": " + (bucket.tryConsume(1) ? "Allowed" : "Denied") + " (Available: " + bucket.getAvailableTokens() + ")");
        }

        System.out.println("\n--- Waiting for refill ---");
        TimeUnit.SECONDS.sleep(2); // Wait 2 seconds, should add 2 * 10 = 20 tokens (capped at 20)

        System.out.println("\n--- Sustained Rate ---");
        // Available should be close to capacity (20) after refill
        System.out.println("Check after wait: Available = " + bucket.getAvailableTokens() + " (Attempting refill implicitly)");
        bucket.tryConsume(1); // Trigger internal refill check
        System.out.println("Check after consume: Available = " + bucket.getAvailableTokens());


        for (int i = 0; i < 15; i++) {
            System.out.println("Request " + (i + 1) + ": " + (bucket.tryConsume(1) ? "Allowed" : "Denied") + " (Available: " + bucket.getAvailableTokens() + ")");
            TimeUnit.MILLISECONDS.sleep(100); // Simulate requests every 100ms (10/sec)
        }

        System.out.println("\n--- Faster Rate (should deny some) ---");
        for (int i = 0; i < 100; i++) {
            System.out.println("Request " + (i + 1) + ": " + (bucket.tryConsume(1) ? "Allowed" : "Denied") + " (Available: " + bucket.getAvailableTokens() + ")");
            TimeUnit.MILLISECONDS.sleep(50); // Simulate requests every 50 ms (20/sec) - faster than refill
        }
    }

    /**
     * Attempts to consume a specified number of tokens from the bucket.
     *
     * @param tokensToConsume The number of tokens to consume. Must be > 0.
     * @return true if tokens were consumed successfully, false otherwise.
     */
    public boolean tryConsume(int tokensToConsume) {
        if (tokensToConsume <= 0) {
            throw new IllegalArgumentException("Number of tokens to consume must be positive");
        }

        lock.lock();
        try {
            refill(); // Add any new tokens accumulated since the last check

            long currentTokens = availableTokens.get();
            if (tokensToConsume <= currentTokens) {
                // Enough tokens available, consume them
                availableTokens.addAndGet(-tokensToConsume);
                return true;
            } else {
                // Not enough tokens
                return false;
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Refills the bucket with tokens accumulated since the last refill time.
     * This method should be called within a critical section (lock).
     */
    private void refill() {
        final long now = System.nanoTime();
        // Get the last refill time atomically
        final long lastRefill = lastRefillTimestampNano.get();

        final long elapsedNanos = now - lastRefill;

        // Only proceed if time has actually advanced.
        if (elapsedNanos <= 0) {
            return; // No time passed, nothing to refill.
        }

        // Calculate how many tokens should have been generated.
        // Use double for intermediate precision with ratePerNano.
        // Ensure we use a non-negative long value for generated tokens.
        final double generatedTokensDouble = elapsedNanos * refillRatePerNano;
        final long generatedTokens = Math.max(0L, (long) generatedTokensDouble);

        // Update token state only if new tokens were generated.
        if (generatedTokens > 0) {
            // Get the current number of tokens atomically.
            final long currentTokens = availableTokens.get();
            // Calculate the new token count: add generated, then cap at capacity.
            // No need to check for fullness beforehand, Math.min handles the cap.
            final long newTokens = Math.min(capacity, currentTokens + generatedTokens);

            // Update the token count. Safe under the external lock from tryConsume.
            availableTokens.set(newTokens);
            lastRefillTimestampNano.set(now);
        }
    }

    /**
     * Returns the current number of available tokens without triggering a refill.
     * Mostly for monitoring or testing.
     *
     * @return Current available tokens.
     */
    public long getAvailableTokens() {
        // Note: Doesn't refill before checking. Could be slightly stale.
        // For an accurate count, call refill() first (but that modifies state).
        return availableTokens.get();
    }

}