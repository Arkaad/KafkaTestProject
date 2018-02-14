package com.testcase.avro;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

/**
 * Created by Arka Dutta on 14-Feb-18.
 */
public class ConsumerResultAvro {
    private final static String TOPIC = "WordsWithCountsTopic";  //Result
    //    private final static String TOPIC = "TextLinesTopic";  //Left
//    private final static String TOPIC = "RekeyedIntermediateTopic";  //Right
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


    static void runConsumer() throws InterruptedException {
        final KafkaConsumer consumer = createConsumer();
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));
        while (true) {
            final ConsumerRecords<String, String> consumerRecords =
                    consumer.poll(200);
            for (ConsumerRecord<String, String> record : consumerRecords) {
                if (record.serializedValueSize() > -1)
                    System.out.println("Consumed Record : " + record.key() + ":" + record.serializedValueSize());
            }
            consumer.commitAsync();
        }
    }

    public static void main(String... args) throws Exception {
        runConsumer();
    }
}
