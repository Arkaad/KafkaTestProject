package com.testcase.avro;

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
    private int diffPerInterval;
    private long safeTime;

    public SampleSimulatorAvro(int intervals, long timeInterval, int limit, int diffPerInterval, long safeTime) {
        this.intervals = intervals;
        this.timeInterval = timeInterval;
        this.limit = limit;
        this.diffPerInterval = diffPerInterval;
        this.safeTime = safeTime;
    }

    public void startProcess(String projectId) throws Exception {
        long windowTime = (timeInterval * (intervals - 1)) + safeTime;
        System.out.println("windowTime = " + windowTime + " ms.");
        System.out.println("projectId = " + projectId);

        AvroProducer producer;
        AvroParser.createAvroSchema();
        JoinStreamAvro stream = new JoinStreamAvro(windowTime, projectId);
        stream.initKStream();
        Thread.sleep(5000); //10 secs
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
            for (j = 1; j <= limit + (i * diffPerInterval); j++) {
                producer.publishData(("COL_ID_" + String.valueOf(j)),
                        AvroParser.getBinaryAvroData("COL_ID", System.currentTimeMillis(), "JOB_ID", i, "1848d5e5-1d3c-4e5e-a7e6-f1d5a19ec00a", "SAMPLE_HASH"), offsetMap);
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
