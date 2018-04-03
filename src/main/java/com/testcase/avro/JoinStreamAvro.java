package com.testcase.avro;

import com.testcase.util.Utility;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.Properties;

/**
 * Created by Arka Dutta on 13-Feb-18.
 */
public class JoinStreamAvro {
    private KafkaStreams streams = null;
    private long windowTime = 0L;

    public JoinStreamAvro(long windowTime) {
        this.windowTime = windowTime;
    }

    public void init() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,
                "join-Kafka-Second-Stream");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                Utility.BOOTSTRAP_SERVERS);
        config.put(StreamsConfig.STATE_DIR_CONFIG, "D:\\TestProjects\\KafkaTest\\kafka_2.11-0.11.0.2\\streams-pipe");

        KStreamBuilder builder = new KStreamBuilder();

        KStream left = builder.stream(Utility.KAFKA_TOPIC_LEFT);
        KStream right = builder.stream(Utility.KAFKA_TOPIC_RIGHT);
        KStream joined = left.outerJoin(right,
                new ValueJoiner() {
                    @Override
                    public Object apply(Object leftValue, Object rightValue) {
                        if (leftValue == null && rightValue != null)
                            return rightValue;
                        else
                            return null;
                    }
                }, /* ValueJoiner */
                JoinWindows.of(windowTime)
        );
        joined.to(Utility.KAFKA_TOPIC_DELTA);
        streams = new KafkaStreams(builder, config);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Closing Kafka Stream");
            if (streams != null) {
                close();
            }
        }));
        try {
            streams.cleanUp();
            streams.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void close() {
        System.out.println("Closing Stream ...");
        if (streams != null) {
            streams.close();
        }
    }

    public static void main(String[] args) {
        JoinStreamAvro obj = new JoinStreamAvro(5 * 60 * 1000);
        obj.init();
    }
}
