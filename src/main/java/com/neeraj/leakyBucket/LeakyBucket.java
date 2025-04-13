package com.neeraj.leakyBucket;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class LeakyBucket {

    private final BlockingQueue<Runnable> blockingQueue; // Hold task or request
    private final ScheduledExecutorService scheduler; // Processes the task from the queue
    private final long processingIntervalNanos; // Delay between processing task

    public LeakyBucket(int capacity, long rate, TimeUnit rateUnit) {
        this.blockingQueue = new ArrayBlockingQueue<>(capacity);

        // Convert the rate at which processing should happen first in Nanos
        long rateUnitNanos = rateUnit.toNanos(1);
        this.processingIntervalNanos = (long) ((double) rateUnitNanos / rate);
        if (this.processingIntervalNanos <= 0) {
            throw new IllegalArgumentException("Rate is too high, resulting in zero or negative values");
        }

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setDaemon(true); // Allow JVM to exit if only this thread is running
            t.setName("LeakyBucket-scheduler");
            return t;
        });

        // Start the leaking
        startLeaking();
    }

    public static void main(String[] args) throws InterruptedException {
        // Example: Capacity 10, process 2 requests per second (interval 500ms)
        int capacity = 10;
        long rate = 2;
        TimeUnit rateUnit = TimeUnit.SECONDS;
        LeakyBucket leakyBucket = new LeakyBucket(capacity, rate, rateUnit);

        AtomicInteger processedCount = new AtomicInteger();
        AtomicInteger rejectedCount = new AtomicInteger();
        AtomicInteger requestCounter = new AtomicInteger();

        // Simulate request arriving burstily (15 request in 1.5 seconds) (10 request/second)
        // Since you are sleeping for 50ms between every request that's 1000ms(in 1 second)/100ms = 10req in 1 second.
        // faster than current processing capacity which is 2 request/second
        Runnable taskProducer = () -> {
            for (int i = 0; i < 15; i++) {
                final int requestId = requestCounter.incrementAndGet();
                Runnable requestTask = () -> {
                    // Simulate processing work
                    System.out.println("Processing request #" + requestId + " at " + System.currentTimeMillis());
                    processedCount.incrementAndGet();
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                };

                if (leakyBucket.tryRequest(requestTask)) {
                    System.out.println("Request #" + requestId + " enqueued @ [" + System.currentTimeMillis() + "] . Queue size: " + leakyBucket.getQueueSize());
                } else {
                    System.out.println("Request #" + requestId + " REJECTED (Queue Full). @ [" + System.currentTimeMillis() + "] Queue size: " + leakyBucket.getQueueSize());
                    rejectedCount.incrementAndGet();
                }

                try {
                    Thread.sleep(100); // Request arrives at every 100ms (10req/second) since 1 second = 1000ms
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };

        Thread producerThread = new Thread(taskProducer);
        producerThread.start();
        producerThread.join(); // Wait for the producer to finish sending

        System.out.println("\n--- Waiting for queue to drain ---");
        // Wait long enough for the queue to likely drain based on processing rate
        // To finish whatever is there in queue, we will take capacity/req_processed_in_1_second
        // So for cap=10 and 2 req/second that's 10/2 = 5 seconds total to finish entire queue
        // +2 second buffer if some req gets processed and new one enters
        long waitTimeMillis = (long) (capacity / (double) rate * rateUnit.toMillis(1)) + 2000; // Estimate + buffer
        System.out.println("(Estimated wait: " + waitTimeMillis + " ms)");
        TimeUnit.MILLISECONDS.sleep(waitTimeMillis);

        System.out.println("\n--- Final Stats ---");
        System.out.println("Total Requests Sent: " + requestCounter.get());
        System.out.println("Total Requests Processed: " + processedCount.get());
        System.out.println("Total Requests Rejected: " + rejectedCount.get());
        System.out.println("Final Queue Size: " + leakyBucket.getQueueSize()); // Should be close to 0

        leakyBucket.shutdown();
        System.out.println("Bucket shut down.");

    }

    /**
     * Shuts down the background processor thread gracefully
     * Waits a short period for existing tasks to complete gracefully
     */
    public void shutdown() {
        scheduler.shutdownNow();
        try {
            // Wait a bit for queued tasks to potentially finish processing
            if (!scheduler.awaitTermination(processingIntervalNanos * 2, TimeUnit.NANOSECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private int getQueueSize() {
        return blockingQueue.size();
    }

    private void startLeaking() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                // take will block if the queue is empty
                Runnable task = blockingQueue.take();
                // Execute immediately, since this scheduler will pull asap
                // as soon as it's got it's chance since scheduler is running at a
                // processingIntervalNanos rate
                task.run();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                System.err.println("LeakyBucket processor interrupted");
            } catch (Exception ex) {
                // Handle exception encountered during task run
                // so that scheduler doesn't break
                System.err.println("Exception processing task: " + ex.getMessage());
                //noinspection CallToPrintStackTrace
                ex.printStackTrace();
            }
        }, 0, processingIntervalNanos, TimeUnit.NANOSECONDS);
    }

    public boolean tryRequest(Runnable task) {
        if (task == null) {
            throw new NullPointerException("Requested task should not be null");
        }
        return blockingQueue.offer(task);
    }

}
