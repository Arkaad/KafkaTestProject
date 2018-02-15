package com.testcase.avro;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Created by Arka Dutta on 14-Feb-18.
 */
public class ProducerRightAvro implements AvroProducer {
    private final static String TOPIC = "RekeyedIntermediateTopic";
    private final static String SERVER = "localhost:9092";
    private KafkaProducer producer = null;

    public static KafkaProducer createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536); //64 KB
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        KafkaProducer producer = new KafkaProducer(props);
        return producer;
    }

    public void init() {
        producer = createProducer();
    }

    public void publishData(String key, byte[] value, Map<Integer, ArrayList<Long>> map) throws ExecutionException, InterruptedException {

        ProducerRecord record = new ProducerRecord<>(TOPIC, key, value);
        RecordMetadata metadata = (RecordMetadata) producer.send(record).get();
        int partition = metadata.partition();
        long offset = metadata.offset();
        ArrayList<Long> list = map.get(partition);
        if (list == null) {
            list = new ArrayList<>();
            list.add(0, offset);
            list.add(1, offset);
            map.put(partition, list);
        } else {
            list.set(1, offset);
        }
//        System.out.printf("sent record(key=%s value=%s) meta(partition=%d, offset=%d) \n", record.key(), record.value(), metadata.partition(), metadata.offset());
    }

    public void close() {
        if (producer != null) {
            producer.flush();
            producer.close();
        }
    }

    static void runProducer() throws Exception {
        final KafkaProducer producer = createProducer();
        long time = System.currentTimeMillis();
        try {


            for (int i = 1; i <= 10; i++) {

                ProducerRecord record = new ProducerRecord<>(TOPIC, String.valueOf(i), AvroParser.getByteArray(String.valueOf(i)));
                RecordMetadata metadata = (RecordMetadata) producer.send(record).get();
                System.out.printf("sent record(key=%s value=%s) meta(partition=%d, offset=%d) \n", record.key(),
                        record.value(), metadata.partition(), metadata.offset());
            }
        } finally {
            producer.flush();
            producer.close();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        try {
            runProducer();
        } catch (Exception e) {
            e.printStackTrace();
        }
        Thread.sleep(3000);
    }
}
