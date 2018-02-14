package com.testcase.avro;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Created by Arka Dutta on 13-Feb-18.
 */
public class StartWorkflowAvro {
    public static void main(String[] args) {
        int interval = 2;
        long intervalTime = 1 * 5 * 1000L; //10 sec
        int limit = 10;//1 M
        try {
            new SampleSimulatorAvro(interval, intervalTime, limit).startProcess();
        } catch (ExecutionException | InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }
}
