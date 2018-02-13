package com.testcase.first;

import java.util.concurrent.ExecutionException;

/**
 * Created by Arka Dutta on 12-Feb-18.
 */
public class StartWorkflow {
    public static void main(String[] args) {
        int interval = 5;
        long intervalTime = 5 * 1000L; //5 secs
        try {
            new SampleSimulator(interval, intervalTime).startProcess();
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
