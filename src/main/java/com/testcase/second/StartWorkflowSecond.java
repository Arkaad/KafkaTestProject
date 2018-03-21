package com.testcase.second;

import java.util.concurrent.ExecutionException;

/**
 * Created by Arka Dutta on 13-Feb-18.
 */
public class StartWorkflowSecond {
    public static void main(String[] args) {
        int interval = 3;
        long intervalTime = 1 * 30 * 1000L; //1 minute
        int limit = 10;//1 K
        try {
            new SampleSimulatorSecond(interval, intervalTime, limit).startProcess();
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
