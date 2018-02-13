package com.testcase.util;

import java.util.concurrent.ExecutionException;

/**
 * Created by Arka Dutta on 12-Feb-18.
 */
public interface Producer {
    public void init();

    public long publishData(String key, String value) throws ExecutionException, InterruptedException;

    public void close();
}
