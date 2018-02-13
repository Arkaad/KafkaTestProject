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
    private long timeInterval;

    public SampleSimulatorSecond(int intervals, long timeInterval) {
        this.intervals = intervals;
        this.timeInterval = timeInterval;
    }

    public void startProcess() throws ExecutionException, InterruptedException {
        long safeTime = 20 * 1000; //10 secs
        long windowTime = (timeInterval * (intervals - 1)) + safeTime;
        System.out.println("windowTime = " + windowTime + " ms.");

        Producer producer;
        int limit = 1000000; //1 million
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
            startOffset = producer.publishData(String.valueOf(j), producer.getClass().getSimpleName() + "_" + j);
            for (; j <= limit + (i * 5); j++) {
                endOffset = producer.publishData(String.valueOf(j), producer.getClass().getSimpleName() + "_" + j);
            }
            System.out.println("End of Publishing data, Time taken : " + (System.currentTimeMillis() - startPublish) + " ms.");
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
