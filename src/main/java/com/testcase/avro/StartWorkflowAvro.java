package com.testcase.avro;

/**
 * Created by Arka Dutta on 13-Feb-18.
 */
public class StartWorkflowAvro {
    public static void main(String[] args) {
        int interval = 3;
        long intervalTime = 1 * 30 * 1000L; //30 mins
        long safeTime = 5 * 60 * 1000L;//5 mins
        int limit = 1000;//1 M
        try {
            new SampleSimulatorAvro(interval, intervalTime, limit).startProcess();
//            new SampleSimulatorAvroUsingDB(interval, intervalTime, safeTime).startProcess();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
