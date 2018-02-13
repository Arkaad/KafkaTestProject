package com.testcase.first;

import com.testcase.util.Producer;
import com.testcase.util.ProducerLeft;
import com.testcase.util.ProducerRight;

import java.util.concurrent.ExecutionException;

/**
 * Created by Arka Dutta on 12-Feb-18.
 */
public class SampleSimulator {
    private int intervals;
    private long timeInterval;

    public SampleSimulator(int intervals, long timeInterval) {
        this.intervals = intervals;
        this.timeInterval = timeInterval;
    }

    public void startProcess() throws ExecutionException, InterruptedException {
        long windowTime = timeInterval + timeInterval / 2L;
        System.out.println("windowTime = " + windowTime + " ms.");

        Producer producer = null;
        int limit = 10;
        JoinStream streamEven = new JoinStream(windowTime, "EVEN");
        JoinStream streamOdd = new JoinStream(windowTime, "ODD");

        for (int i = 0; i < intervals; i++) {
            System.out.println("Sampling Data for : " + i + " interval");
            if (i % 2 == 0) {
                producer = new ProducerLeft();
                streamEven.init();
            } else {
                producer = new ProducerRight();
                streamOdd.init();
            }
            producer.init();
            for (int j = 1; j <= limit + (i * 5); j++) {
                producer.publishData(String.valueOf(j), producer.getClass().getSimpleName() + "_" + j);
            }
            producer.close();
            Thread.sleep(1000);
            if (i % 2 == 0) {
                streamOdd.close();
            } else {
                streamEven.close();
            }

            if (i + 1 == intervals) {
                break;
            }
            System.out.println("Scheduled for next Interval.. !!");
            Thread.sleep(timeInterval);
        }
        Thread.sleep(2 * 1000);
        streamEven.close();
        streamOdd.close();
    }
}
