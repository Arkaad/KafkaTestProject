package com.testcase.first;

import com.testcase.util.Utility;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.time.Duration;
import java.util.Properties;

/**
 * Created by Arka Dutta on 12-Feb-18.
 */
public class JoinStream {
    private KafkaStreams streams = null;
    private long windowTime = 0L;
    private String applicationId = "";

    public JoinStream(long windowTime, String applicationId) {
        this.windowTime = windowTime;
        this.applicationId = applicationId;
    }

    public void init() {
        System.out.println("Starting " + applicationId + "....");
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,
                applicationId);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                Utility.BOOTSTRAP_SERVERS);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());


        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> left = builder.stream(Utility.KAFKA_TOPIC_LEFT);
        KStream<String, String> right = builder.stream(Utility.KAFKA_TOPIC_RIGHT);
        KStream joined;
        if (applicationId.equals("ODD")) {
            joined = right.outerJoin(left,
                    new ValueJoiner() {
                        @Override
                        public Object apply(Object leftValue, Object rightValue) {
//                            return "left=" + leftValue + ", right=" + rightValue + "   - " + applicationId;
                            if (leftValue == null && rightValue != null)
                                return "left=" + leftValue + ", right=" + rightValue + "   - " + applicationId;
                            else
                                return null;
                        }
                    }, /* ValueJoiner */
                    JoinWindows.of(Duration.ofMillis(windowTime))
            );
        } else {
            joined = left.outerJoin(right,
                    new ValueJoiner() {
                        @Override
                        public Object apply(Object leftValue, Object rightValue) {
//                            return "left=" + leftValue + ", right=" + rightValue + "   - " + applicationId;
                            if (leftValue == null && rightValue != null)
                                return "left=" + null + ", right=" + rightValue + "   - " + applicationId;
                            else
                                return null;
                        }
                    }, /* ValueJoiner */
                    JoinWindows.of(Duration.ofMillis(windowTime))
            );
        }

        joined.to(Utility.KAFKA_TOPIC_DELTA);
        streams = new KafkaStreams(builder.build(), config);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Closing Kafka Stream");
            if (streams != null) {
                streams.close();
            }
        }));
        try {
//            streams.cleanUp();
            streams.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void close() {
        System.out.println("Closing " + applicationId + "...");
        if (streams != null) {
            streams.close();
        }
    }
}
