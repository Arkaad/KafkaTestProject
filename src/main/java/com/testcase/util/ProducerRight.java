package com.testcase.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Created by Arka Dutta on 07-Feb-18.
 */
public class ProducerRight implements Producer {
    private final static String TOPIC = Utility.KAFKA_TOPIC_RIGHT;
    private final static String SERVER = Utility.BOOTSTRAP_SERVERS;
    private KafkaProducer producer = null;

    public static KafkaProducer createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536); //64 KB
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer producer = new KafkaProducer(props);
        return producer;
    }

    public void init() {
        producer = createProducer();
    }

    public long publishData(String key, String value) throws ExecutionException, InterruptedException {

        ProducerRecord record = new ProducerRecord<>(TOPIC, key, value);
        RecordMetadata metadata = (RecordMetadata) producer.send(record).get();
        System.out.printf("sent record(key=%s value=%s) meta(partition=%d, offset=%d) \n", record.key(), record.value(), metadata.partition(), metadata.offset());
        return metadata.offset();
    }

    public void close() {
        if (producer != null) {
            producer.flush();
            producer.close();
        }
    }

    static void runProducer(final int sendMessageCount) throws Exception {
        final KafkaProducer producer = createProducer();
        long time = System.currentTimeMillis();
        try {


//            ProducerRecord record = new ProducerRecord<>(TOPIC, time, "hello kafka streams");
//            RecordMetadata metadata = (RecordMetadata) producer.send(record).get();
//            System.out.printf("sent record(key=%s value=%s) meta(partition=%d, offset=%d) \n", record.key(), record.value(), metadata.partition(), metadata.offset());
//
//            record = new ProducerRecord<>(TOPIC, time++, "all streams lead to kafka");
//            metadata = (RecordMetadata) producer.send(record).get();
//            System.out.printf("sent record(key=%s value=%s) meta(partition=%d, offset=%d) \n", record.key(), record.value(), metadata.partition(), metadata.offset());
//
//            record = new ProducerRecord<>(TOPIC, time++, "join kafka summit");
//            metadata = (RecordMetadata) producer.send(record).get();
//            System.out.printf("sent record(key=%s value=%s) meta(partition=%d, offset=%d) \n", record.key(), record.value(), metadata.partition(), metadata.offset());

            for (int i = 1; i <= 15; i++) {

                ProducerRecord record = new ProducerRecord<>(TOPIC, String.valueOf(i), "value_" + i);
                RecordMetadata metadata = (RecordMetadata) producer.send(record).get();
                System.out.printf("sent record(key=%s value=%s) meta(partition=%d, offset=%d) \n", record.key(), record.value(), metadata.partition(), metadata.offset());
            }
        } finally {
            producer.flush();
            producer.close();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        try {
            runProducer(5);
        } catch (Exception e) {
            e.printStackTrace();
        }
        Thread.sleep(3000);
    }
}
