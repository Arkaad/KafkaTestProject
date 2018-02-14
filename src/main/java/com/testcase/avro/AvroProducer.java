package com.testcase.avro;

import java.util.concurrent.ExecutionException;

/**
 * Created by Arka Dutta on 12-Feb-18.
 */
public interface AvroProducer {
    public void init();

    public long publishData(String key, byte[] value) throws ExecutionException, InterruptedException;

    public void close();
}
