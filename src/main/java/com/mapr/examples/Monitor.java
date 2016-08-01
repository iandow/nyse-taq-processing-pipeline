package com.mapr.examples;

/**
 * Created by idownard on 7/27/16.
 */
public class Monitor {
    public static void print_status(long records_processed, int poolSize, long startTime) {
        long elapsedTime = System.nanoTime() - startTime;
        System.out.printf("Throughput = %.2f Kmsgs/sec published. Threads = %d. Total published = %d\n", records_processed / ((double) elapsedTime / 1000000000.0) / 1000, poolSize, records_processed);
    }
}
