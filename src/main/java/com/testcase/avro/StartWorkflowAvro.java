package com.testcase.avro;

import org.apache.kafka.streams.kstream.JoinWindows;

import java.util.UUID;

/**
 * Created by Arka Dutta on 13-Feb-18.
 */
public class StartWorkflowAvro {
    public static void main(String[] args) {
        int interval = 3;
        long intervalTime = 1 * 30 * 1000L; //30 sec
        int limit = 10;
        int diffPerInterval = 5;
        long safeTime = 30 * 60 * 1000L;//30 mins
        try {
            String projectId = UUID.randomUUID().toString();
            CheckOrCreateTopic.checkOrCreateTopic();

            new SampleSimulatorAvro(interval, intervalTime, limit, diffPerInterval, safeTime).startProcess(projectId);
//            JoinWindows joinWindows = JoinWindows.of(safeTime).after(0);
//            System.out.println(joinWindows.size());

//            new SampleSimulatorAvroUsingDB(interval, intervalTime, safeTime).startProcess(projectId);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
