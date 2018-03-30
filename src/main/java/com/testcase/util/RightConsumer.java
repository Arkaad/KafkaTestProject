package com.testcase.util;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

/**
 * Created by Arka Dutta on 07-Feb-18.
 */
public class RightConsumer {
    private final static String TOPIC = "kafka-test-right";
    private final static String SERVER = KafkaConfig.BOOTSTRAP_SERVERS;

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


    static void runConsumer() throws InterruptedException {
        final KafkaConsumer consumer = createConsumer();
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));
        while (true) {
            final ConsumerRecords<String, String> consumerRecords =
                    consumer.poll(200);
            for (ConsumerRecord<String, String> record : consumerRecords) {
                if (record.value() != null)
                    System.out.println("Consumed Record : " + record.key() + ":" + record.value());
            }
            consumer.commitAsync();
        }
    }

    public static void main(String... args) throws Exception {
        runConsumer();
    }
}
