package com.testcase.avro;

import com.testcase.util.DatabaseConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by Arka Dutta on 13-Feb-18.
 */
public class SampleSimulatorAvroUsingDB {
    private int intervals;
    private long timeInterval;
    private final long colId = 3304312411574666869L;
    private final long prjId = -454541316L;
    private final long jobId = 1245454L;
    private final String hash = "ABCDEFGHIJKLMNOP123456QR";
    private long safeTime = 0L;

    public SampleSimulatorAvroUsingDB(int intervals, long timeInterval, long safeTime) {
        this.intervals = intervals;
        this.timeInterval = timeInterval;
        this.safeTime = safeTime;
    }

    public void startProcess() throws Exception {
        long windowTime = (timeInterval * (intervals - 1)) + safeTime;
        System.out.println("windowTime = " + windowTime + " ms. " + (windowTime / 60000) + " mins");
        String sql;
        PreparedStatement ps = null;
        Connection conn = null;
        ResultSet rs = null;
        AvroProducer producer;
        JoinStreamAvro stream = new JoinStreamAvro(windowTime);
        stream.init();
        for (int i = 0; i < intervals; i++) {
            System.out.println("Sampling Data for : " + i + " interval");
            producer = i > 0 ? new ProducerRightAvro() : new ProducerLeftAvro();
            producer.init();

            Map<Integer, ArrayList<Long>> offsetMap = new HashMap<>();
            long count = 0L;
            long startPublish = 0L;
            sql = "SELECT TEST_ID FROM LINEAGE_TEST_" + i;
            System.out.println("sql = " + sql);
            try {
                conn = DatabaseConnection.getDBConnection();
                ps = conn.prepareStatement(sql);
                rs = ps.executeQuery();
                startPublish = System.currentTimeMillis();
                while (rs.next()) {
                    count++;
                    StringBuilder key = new StringBuilder(String.valueOf(colId));
                    key.append("_").append(hash).append("-").append(rs.getString(1));
                    producer.publishData(key.toString(), AvroParser.getBinaryAvroData(startPublish, prjId, jobId, i), offsetMap);
                    if (count % 5000 == 0) {
                        System.out.println(count + " data Published");
                    }
                }
            } finally {
                if (rs != null) {
                    rs.close();
                }
                if (ps != null) {
                    ps.close();
                }
                if (conn != null) {
                    conn.close();
                }
            }
            System.out.println("End of Publishing " + (count) + " data, Time taken : " + (System.currentTimeMillis() - startPublish) + " ms.");
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
