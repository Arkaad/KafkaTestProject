package com.testcase.second;

import java.util.concurrent.ExecutionException;

/**
 * Created by Arka Dutta on 13-Feb-18.
 */
public class StartWorkflowSecond {
    public static void main(String[] args) {
        int interval = 5;
        long intervalTime = 8 * 1000L; //8 secs
        try {
            new SampleSimulatorSecond(interval, intervalTime).startProcess();
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
