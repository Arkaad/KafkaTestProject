package com.testcase.test;

import com.testcase.avro.AvroParser;
import com.testcase.util.Utility;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

/**
 * Created by Arka Dutta on 20-Mar-18.
 */
public class KafkaDemoConsumer {
    //    private final static String TOPIC = "DELTA-c35df20e-7dae-4174-8dc4-a3c81c9018b8";
    private final static String TOPIC = "lineage-delta";
    private final static String SERVER = Utility.BOOTSTRAP_SERVERS;
    private final static long POLL_INTERVAL = 5 * 1000;  //5 secs

    private static KafkaConsumer createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                SERVER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "KafkaExampleConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        // Create the consumer using props.
        KafkaConsumer consumer = new KafkaConsumer(props);
        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }

    private static void runConsumer() throws InterruptedException, IOException {
        final KafkaConsumer consumer = createConsumer();
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));
        while (true) {
            final ConsumerRecords<String, byte[]> consumerRecords = consumer.poll(POLL_INTERVAL);
            for (ConsumerRecord<String, byte[]> record : consumerRecords) {
                if (record.serializedValueSize() > -1) {
                    AvroParser.deserialize(record.value());
                }
            }
            consumer.commitAsync();
        }
    }

    public static void main(String... args) throws Exception {
        runConsumer();
    }

}
