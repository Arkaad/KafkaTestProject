package com.testcase.avro;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by Arka Dutta on 13-Feb-18.
 */
public class SampleSimulatorAvro {
    private int intervals;
    private int limit;
    private long timeInterval;

    public SampleSimulatorAvro(int intervals, long timeInterval, int limit) {
        this.intervals = intervals;
        this.timeInterval = timeInterval;
        this.limit = limit;
    }

    public void startProcess() throws ExecutionException, InterruptedException, IOException, IOException {
        long safeTime = 20 * 1000; //20 secs
        long windowTime = (timeInterval * (intervals - 1)) + safeTime;
        System.out.println("windowTime = " + windowTime + " ms.");

        AvroProducer producer;
        JoinStreamAvro stream = new JoinStreamAvro(windowTime);
        stream.init();
        for (int i = 0; i < intervals; i++) {
            System.out.println("Sampling Data for : " + i + " interval");

            if (i > 0) {
                producer = new ProducerRightAvro();
            } else {
                producer = new ProducerLeftAvro();
            }
            producer.init();
            System.out.println("Publishing Data ......");
            long startPublish = System.currentTimeMillis();
            Map<Integer, ArrayList<Long>> offsetMap = new HashMap<>();
            int j;
            for (j = 1; j <= limit + (i * 5); j++) {
                producer.publishData(("ABCDEFGHIJKLMNOP123456QRST458692_2589631478456932147895632147_" + String.valueOf(j)),
                        AvroParser.getByteArray(i), offsetMap);
                if (j % 5000 == 0) {
                    System.out.println(j + " data Published");
                }
            }
            System.out.println("End of Publishing " + (j - 1) + " data, Time taken : " + (System.currentTimeMillis() - startPublish) + " ms.");
            producer.close();
            if (i + 1 == intervals) {
                break;
            }
            Thread.sleep(2 * 1000);
            if (i > 0) {
                System.out.println("Copying Data with Offset Map = " + offsetMap + " Interval = " + i + " .........");
                long startCopy = System.currentTimeMillis();
                long copyCount = copyTopic(offsetMap);
                System.out.println("End of Copying Data .. Time taken to copy " + copyCount + " data is : " + (System.currentTimeMillis() - startCopy) + " ms.");
            }

            System.out.println("Scheduled for next Interval.. " + (i + 1) + " !!");
            Thread.sleep(timeInterval);
        }
        Thread.sleep(2 * 1000);
        stream.close();
    }

    private long copyTopic(Map<Integer, ArrayList<Long>> offsetMap) throws ExecutionException, InterruptedException {
        long count = 0L;
        for (Map.Entry<Integer, ArrayList<Long>> integerArrayListEntry : offsetMap.entrySet()) {
            count += new AvroCopyRightToLeftTopic().copyData(integerArrayListEntry.getKey(), integerArrayListEntry.getValue().get(0), integerArrayListEntry.getValue().get(1));
        }
        return count;
    }
}
