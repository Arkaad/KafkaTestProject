package com.testcase.avro;

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
 * Created by Arka Dutta on 14-Feb-18.
 */
public class ConsumerResultAvro {
    private final static String TOPIC = Utility.KAFKA_TOPIC_DELTA;  //Result
    //    private final static String TOPIC = Utility.KAFKA_TOPIC_LEFT;  //Left
//    private final static String TOPIC = Utility.KAFKA_TOPIC_RIGHT;  //Right
    private final static String SERVER = Utility.BOOTSTRAP_SERVERS;

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
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                consumer.close();
            }
        }));
        while (true) {
            final ConsumerRecords<String, byte[]> consumerRecords =
                    consumer.poll(200);
            for (ConsumerRecord<String, byte[]> record : consumerRecords) {
                if (record.serializedValueSize() > -1 && !record.key().equals(CheckOrCreateTopic.getKey())) {
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
