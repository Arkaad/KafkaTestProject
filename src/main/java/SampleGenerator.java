import com.testcase.util.Producer;
import com.testcase.util.ProducerLeft;
import com.testcase.util.ProducerRight;

import java.util.concurrent.ExecutionException;

/**
 * Created by Arka Dutta on 12-Feb-18.
 */
public class SampleGenerator {

    private int intervals;
    private long timeInterval;

    public SampleGenerator(int intervals, long timeInterval) {
        this.intervals = intervals;
        this.timeInterval = timeInterval;
    }

    public void startProcess() throws ExecutionException, InterruptedException {
        Producer producer;
        int limit = 10;
        JoinKafkaStream stream = new JoinKafkaStream();
        long windowTime = timeInterval + timeInterval / 2L;
        System.out.println("windowTime = " + windowTime + " ms.");
        stream.init(windowTime);
        for (int i = 0; i < intervals; i++) {
            System.out.println("Sampling Data for : " + i + " interval");
            producer = i % 2 == 0 ? new ProducerLeft() : new ProducerRight();
            producer.init();
            for (int j = 1; j <= limit + (i * 10); j++) {
                producer.publishData(String.valueOf(j), producer.getClass().getName() + "_" + j);
            }
            producer.close();
            if (i + 1 == intervals) {
                break;
            }
            System.out.println("Scheduled for next Interval.. !!");
            Thread.sleep(timeInterval);
        }
        Thread.sleep(2 * 1000);
        stream.close();
    }
}
