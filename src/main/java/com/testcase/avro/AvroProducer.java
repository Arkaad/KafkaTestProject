package com.testcase.avro;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by Arka Dutta on 12-Feb-18.
 */
public interface AvroProducer {
    public void init();

    public void publishData(String key, byte[] value, Map<Integer, ArrayList<Long>> map) throws ExecutionException, InterruptedException;

    public void close();
}
