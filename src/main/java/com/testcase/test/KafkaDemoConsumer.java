package com.testcase.test;

import com.testcase.util.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

/**
 * Created by Arka Dutta on 20-Mar-18.
 */
public class KafkaDemoConsumer {
//    private final static String TOPIC = "DELTA-c35df20e-7dae-4174-8dc4-a3c81c9018b8";
    private final static String TOPIC = "lng-right-af6f0296-e426-4f4e-9e98-f8bea212b13e";
    private final static String SERVER = KafkaConfig.BOOTSTRAP_SERVERS;
    private final static long POLL_INTERVAL = 5 * 1000;  //5 secs

    private static KafkaConsumer createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                SERVER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "KafkaExampleConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
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
            final ConsumerRecords<String, String> consumerRecords = consumer.poll(POLL_INTERVAL);
            for (ConsumerRecord<String, String> record : consumerRecords) {
                System.out.println("Consumed Record : Key -> " + record.key() + " Value -> " + record.value());
            }
            consumer.commitAsync();
        }
    }

    public static void main(String... args) throws Exception {
        runConsumer();
    }

}
