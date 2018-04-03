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
 * Created by Arka Dutta on 07-Feb-18.
 */
public class LeftConsumerAvro {
    private final static String TOPIC = Utility.KAFKA_TOPIC_LEFT;
    private final static String SERVER = Utility.BOOTSTRAP_SERVERS;
    private final static long POLL_TIMEOUT = 5 * 1000;  //5 secs

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
        long count = 0L;
        long loopCount = 0L;
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));
        while (true) {
            final ConsumerRecords<String, byte[]> consumerRecords = consumer.poll(POLL_TIMEOUT);
            for (ConsumerRecord<String, byte[]> record : consumerRecords) {
                if (record.value() != null) {
                    count++;
                    loopCount++;
                    if (count % 5000 == 0) {
                        System.out.println("Consumed Record : Key -> " + record.key() + " ValueMap -> " +
                                AvroParser.getDeserializedAvroDataMap(record.value()));
                        System.out.println("count = " + count);
                    }
                }
            }
            System.out.println("loopCount = " + loopCount);
            consumer.commitAsync();
            loopCount = 0L;
        }
    }

    public static void main(String... args) throws Exception {
        runConsumer();
    }
}
