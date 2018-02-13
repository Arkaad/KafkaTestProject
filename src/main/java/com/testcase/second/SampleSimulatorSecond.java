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
        long safeTime = 10 * 1000; //10 secs
        long windowTime = (timeInterval * (intervals - 1)) + safeTime;
        System.out.println("windowTime = " + windowTime + " ms.");

        Producer producer;
        int limit = 10;
        long startOffset = 0L;
        long endOffset = 0L;
        JoinStreamSecond stream = new JoinStreamSecond(windowTime);
        stream.init();
        for (int i = 0; i < intervals; i++) {
            System.out.println("Sampling Data for : " + i + " interval");
            startOffset = 0L;
            endOffset = 0L;

            if (i > 0) {
                producer = new ProducerRight();
            } else {
                producer = new ProducerLeft();
            }
            producer.init();
            for (int j = 1; j <= limit + (i * 5); j++) {
                long offset = producer.publishData(String.valueOf(j), producer.getClass().getSimpleName() + "_" + j);
                if (startOffset == 0) {
                    startOffset = offset;
                } else {
                    endOffset = offset;
                }
            }
            producer.close();
            Thread.sleep(2 * 1000);
            if (i > 0 && ((i + 1) != intervals)) {
                System.out.println("startOffset = " + startOffset + " endOffset = " + endOffset + " Interval = " + i);
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
