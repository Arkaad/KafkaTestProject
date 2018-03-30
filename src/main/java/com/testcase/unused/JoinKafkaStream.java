package com.testcase.unused;

import com.testcase.util.KafkaConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.Properties;

/**
 * Created by Arka Dutta on 12-Feb-18.
 */
public class JoinKafkaStream {
    private KafkaStreams streams = null;

    public void init(long windowTime) {
        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG,
                "table-join-kafka-streams");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                KafkaConfig.BOOTSTRAP_SERVERS);
        config.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());
        config.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());

        KStreamBuilder builder = new KStreamBuilder();

        final Serde<String> stringSerde = Serdes.String();

        KStream left = builder.stream(stringSerde, stringSerde, "kafka-test-left");
        KStream right = builder.stream(stringSerde, stringSerde, "kafka-test-right");
        KStream joined = left.outerJoin(right,
                new ValueJoiner() {
                    @Override
                    public Object apply(Object leftValue, Object rightValue) {
                        if (leftValue == null && rightValue != null)
                            return "left=" + null + ", right=" + rightValue;
                        else
                            return null;
                    }
                }, /* ValueJoiner */
                JoinWindows.of(windowTime)
        );
        joined.to(stringSerde, stringSerde, "kafka-test-result");
        streams = new KafkaStreams(builder, config);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Closing Kafka Stream");
            if (streams != null) {
                streams.close();
            }
        }));
        streams.cleanUp();
        streams.start();
    }

    public void close() {
        System.out.println("Closing Kafka Stream");
        if (streams != null) {
            streams.close();
        }
    }
}
