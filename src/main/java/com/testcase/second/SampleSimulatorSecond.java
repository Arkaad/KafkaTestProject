package com.testcase.second;

import com.testcase.util.Producer;
import com.testcase.util.ProducerLeft;
import com.testcase.util.ProducerRight;

import java.util.concurrent.ExecutionException;

/**
 * Created by Arka Dutta on 13-Feb-18.
 */
public class SampleSimulatorSecond {
    private int intervals;
    private int limit;
    private long timeInterval;

    public SampleSimulatorSecond(int intervals, long timeInterval, int limit) {
        this.intervals = intervals;
        this.timeInterval = timeInterval;
        this.limit = limit;
    }

    public void startProcess() throws ExecutionException, InterruptedException {
        long safeTime = 20 * 1000; //20 secs
        long windowTime = (timeInterval * (intervals - 1)) + safeTime;
        System.out.println("windowTime = " + windowTime + " ms.");

        Producer producer;
        long startOffset = 0L;
        long endOffset = 0L;
        JoinStreamSecond stream = new JoinStreamSecond(windowTime);
        stream.init();
        for (int i = 0; i < intervals; i++) {
            System.out.println("Sampling Data for : " + i + " interval");

            if (i > 0) {
                producer = new ProducerRight();
            } else {
                producer = new ProducerLeft();
            }
            producer.init();
            System.out.println("Publishing Data ......");
            long startPublish = System.currentTimeMillis();
            int j = 1;
            startOffset = producer.publishData(("ABCDEFGHIJKLMNOP123456QRST458692_25896314784569321478956321478" + String.valueOf(j)), producer.getClass().getSimpleName() + "_" + j);
            for (; j <= limit + (i * 5); j++) {
                endOffset = producer.publishData(("ABCDEFGHIJKLMNOP123456QRST458692_25896314784569321478956321478" + String.valueOf(j)), producer.getClass().getSimpleName() + "_" + j);
                if (j % 5000 == 0) {
                    System.out.println(j + " data Published");
                }
            }
            System.out.println("End of Publishing " + (j - 1) + " data, Time taken : " + (System.currentTimeMillis() - startPublish) + " ms.");
            producer.close();
            Thread.sleep(2 * 1000);
            if (i > 0 && ((i + 1) != intervals)) {
                System.out.println("Copying Data with startOffset = " + startOffset + " endOffset = " + endOffset + " Interval = " + i + " .........");
                copyTopic(startOffset, endOffset);
                System.out.println("End of Copying Data ....................");
            }


            if (i + 1 == intervals) {
                break;
            }
            System.out.println("Scheduled for next Interval.. " + (i + 1) + "!!");
            Thread.sleep(timeInterval);
        }
        Thread.sleep(2 * 1000);
        stream.close();
    }

    private void copyTopic(long startOffset, long endOffset) throws ExecutionException, InterruptedException {
        CopyRightToLeftTopic.copyData(startOffset, endOffset);
    }
}
