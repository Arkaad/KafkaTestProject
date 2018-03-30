package com.testcase.avro;

import com.testcase.util.KafkaConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Created by Arka Dutta on 13-Feb-18.
 */
public class AvroCopyRightToLeftTopic {
    private final static String RIGHT_TOPIC = "kafka-test-right";
    private final static String LEFT_TOPIC = "kafka-test-left";
    private final static String KAFKA_SERVER = KafkaConfig.BOOTSTRAP_SERVERS;

    private KafkaConsumer createRightConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-test-consumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        KafkaConsumer consumer = new KafkaConsumer(props);
        // Subscribe to the topic.
//        consumer.subscribe(Collections.singletonList(RIGHT_TOPIC));

        return consumer;
    }

    private KafkaProducer createLeftProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return new KafkaProducer(props);
    }

    public long copyData(int partition, long startOffset, long endOffset) throws ExecutionException, InterruptedException {
        if (startOffset > endOffset)
            throw new IllegalStateException("Start Offset Cannot be greater than End Offset");
        boolean breakFlag = false;
        long count = 0L;
        final KafkaConsumer consumer = createRightConsumer();
        KafkaProducer producer = createLeftProducer();
        TopicPartition topicPartition = new TopicPartition(RIGHT_TOPIC, partition);
        consumer.assign(Collections.singleton(topicPartition));
        consumer.seek(topicPartition, startOffset);
        System.out.println("Copying data for partition = " + partition + " startOffset = " + startOffset + " endOffset = " + endOffset);
        long start = System.currentTimeMillis();
        while (true) {
            ConsumerRecords<String, byte[]> consumerRecords = consumer.poll(100);
            for (ConsumerRecord<String, byte[]> record : consumerRecords) {
                if (record.value() != null) {
                    producer.send(new ProducerRecord<>(LEFT_TOPIC, record.key(), record.value()));
                    count++;
                }
                if (record.offset() >= endOffset) {
                    breakFlag = true;
                    break;
                }
                if (count % 5000 == 0) {
                    System.out.println(count + " records copied");
                }
            }
            if (breakFlag) {
                consumer.close();
                producer.close();
                break;
            }
        }
        System.out.println("Total : " + count + " records copied in " + (System.currentTimeMillis() - start) + " ms.");
        return count;
    }

    public static void main(String[] args) {
        try {
            new AvroCopyRightToLeftTopic().copyData(0, 8474, 8819);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
