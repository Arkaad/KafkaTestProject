package com.testcase.unused;

import com.testcase.util.Utility;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

/**
 * Created by Arka Dutta on 08-Feb-18.
 */
public class KTableJoin {

    public static void main(String[] args) throws Exception {

        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG,
                "table-join-kafka-streams");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                Utility.BOOTSTRAP_SERVERS);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
//        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        StreamsBuilder builder = new StreamsBuilder();

        KTable left = builder.table(Utility.KAFKA_TOPIC_LEFT);
        KTable right = builder.table(Utility.KAFKA_TOPIC_RIGHT);
        KTable joined = left.join(right,
                (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue);
        joined.toStream().to(Utility.KAFKA_TOPIC_DELTA);

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));


    }
}
