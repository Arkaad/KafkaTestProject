package com.testcase.second;

import com.testcase.util.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Created by Arka Dutta on 13-Feb-18.
 */
public class CopyRightToLeftTopic {
    private final static String RIGHT_TOPIC = "kafka-test-right";
    private final static String LEFT_TOPIC = "kafka-test-left";
    private final static String SERVER = KafkaConfig.BOOTSTRAP_SERVERS;

    private static KafkaConsumer createRightConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                SERVER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "RightConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // Create the consumer using props.
        KafkaConsumer consumer = new KafkaConsumer(props);
        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(RIGHT_TOPIC));
        return consumer;
    }

    private static KafkaProducer createLeftProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", SERVER);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        KafkaProducer producer = new KafkaProducer(props);
        return producer;
    }

    public static void copyData(long startOffset, long endOffset) throws ExecutionException, InterruptedException {
        boolean breakFlag = false;
        long count = 0L;
        final KafkaConsumer consumer = createRightConsumer();
        KafkaProducer producer = createLeftProducer();
        consumer.poll(100);
        consumer.seek(new TopicPartition(RIGHT_TOPIC, 0), startOffset);
        long start = System.currentTimeMillis();
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
            for (ConsumerRecord<String, String> record : consumerRecords) {
                if (record.value() != null) {
                    producer.send(new ProducerRecord<>(LEFT_TOPIC, record.key(), record.value()));
                    count++;
                    if (record.offset() >= endOffset) {
                        breakFlag = true;
                        break;
                    }
                }
                if (count % 5000 == 0) {
                    System.out.println(count + " records copied");
                }
            }
            if (breakFlag) {
                consumer.close();
                break;
            }
        }
        System.out.println("Total : " + count + " records copied in " + (System.currentTimeMillis() - start) + " ms.");
    }

    public static void main(String[] args) {
        try {
            copyData(700, 714);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
