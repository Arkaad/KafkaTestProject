package com.testcase.avro;

import com.testcase.util.Utility;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.Properties;
import java.util.UUID;

/**
 * Created by Arka Dutta on 13-Feb-18.
 */
public class JoinStreamAvro {
    private KafkaStreams streams = null;
    private long windowTime = 0L;
    private String projectId;

    public JoinStreamAvro(long windowTime, String projectId) {
        this.windowTime = windowTime;
        this.projectId = projectId;
    }

    public void initKStream() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,
                projectId);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                Utility.BOOTSTRAP_SERVERS);
        config.put(StreamsConfig.STATE_DIR_CONFIG, ".\\logs\\streams-pipe");

//        KStreamBuilder builder = new KStreamBuilder();
        StreamsBuilder builder = new StreamsBuilder();

//        KStream left = builder.stream(TopologyBuilder.AutoOffsetReset.LATEST, Utility.KAFKA_TOPIC_LEFT);
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
                JoinWindows.of(windowTime).after(0)
        );
        joined.to(Utility.KAFKA_TOPIC_DELTA);
        streams = new KafkaStreams(builder.build(), config);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Closing Kafka Stream");
            if (streams != null) {
                close();
            }
        }));
        try {
            streams.cleanUp();
            streams.start();
            System.out.println("Kafka Streams started ....");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void initKTable() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,
                projectId);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                Utility.BOOTSTRAP_SERVERS);
        config.put(StreamsConfig.STATE_DIR_CONFIG, "E:\\Kafka\\kafka_2.12-1.0.1\\table-pipe");

        StreamsBuilder builder = new StreamsBuilder();
        KTable left = builder.table(Utility.KAFKA_TOPIC_LEFT);
        KTable right = builder.table(Utility.KAFKA_TOPIC_RIGHT);
        KStream joined = left.outerJoin(right, new ValueJoiner() {
            @Override
            public Object apply(Object leftValue, Object rightValue) {
                if (leftValue == null && rightValue != null)
                    return rightValue;
                else
                    return null;
            }
        }).toStream();
        joined.to(Utility.KAFKA_TOPIC_DELTA);

        streams = new KafkaStreams(builder.build(), config);
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

    public void initKStream_V2() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,
                projectId);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                Utility.BOOTSTRAP_SERVERS);
        config.put(StreamsConfig.STATE_DIR_CONFIG, "E:\\Kafka\\kafka_2.12-1.0.1\\streams-pipe");

//        KStreamBuilder builder = new KStreamBuilder();
        StreamsBuilder builder = new StreamsBuilder();

//        KStream left = builder.stream(TopologyBuilder.AutoOffsetReset.LATEST, Utility.KAFKA_TOPIC_LEFT);
        KStream left = builder.stream(Utility.KAFKA_TOPIC_LEFT);
        KStream right = builder.stream(Utility.KAFKA_TOPIC_RIGHT);
        KStream joined = left.join(right, new ValueJoiner() {
                    @Override
                    public Object apply(Object value1, Object value2) {
                        return value1;
                    }
                },
                JoinWindows.of(windowTime).after(0)
        );
        joined.to(Utility.KAFKA_TOPIC_DELTA);
        streams = new KafkaStreams(builder.build(), config);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Closing Kafka Stream");
            if (streams != null) {
                close();
            }
        }));
        try {
            streams.cleanUp();
            streams.start();
            System.out.println("Kafka Streams started ....");
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
        JoinStreamAvro obj = new JoinStreamAvro(5 * 60 * 1000, UUID.randomUUID().toString());
        obj.initKStream();
    }
}
