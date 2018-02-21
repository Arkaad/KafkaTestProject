package com.testcase.avro;

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
 * Created by Arka Dutta on 07-Feb-18.
 */
public class LeftConsumerAvro {
    private final static String TOPIC = "TextLinesTopic";
    private final static String SERVER = "localhost:9092";

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


    static void runConsumer() throws InterruptedException, IOException {
        final KafkaConsumer consumer = createConsumer();
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));
        while (true) {
            final ConsumerRecords<String, byte[]> consumerRecords =
                    consumer.poll(500);
            for (ConsumerRecord<String, byte[]> record : consumerRecords) {
                if (record.value() != null) {
                    System.out.print("Consumed Record : Key -> " + record.key() + " Value-> ");
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
