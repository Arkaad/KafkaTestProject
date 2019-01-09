package com.testcase.unused;

import com.testcase.util.Utility;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Created by Arka Dutta on 08-Feb-18.
 */
public class KStreamJoin {

    public static void main(String[] args) {

        Properties streamsConfiguration = new Properties();
//        streamsConfiguration.put("application.id", "wordcount-lambda-example");
        streamsConfiguration.put("bootstrap.servers", Utility.BOOTSTRAP_SERVERS);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2181");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-kafka-project-app");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> textLines = builder.stream(Utility.KAFKA_TOPIC_LEFT);
        Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);

//        KStream<String, Long> wordCounts = textLines
//                .flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
//                .map(((key, value) -> new KeyValue<String, String>(value, value)))
//                .through(Utility.KAFKA_TOPIC_RIGHT)
//                .countByKey("Counts").toStream();
//
//        wordCounts.to(stringSerde, longSerde, Utility.KAFKA_TOPIC_DELTA);
        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        System.out.println("starting kafka streams...");
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
