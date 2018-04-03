package com.testcase.test;

import com.testcase.util.Utility;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Created by Arka Dutta on 20-Mar-18.
 */
public class KafkaDemoProducer {
    private final static String TOPIC = "lng-right-af6f0296-e426-4f4e-9e98-f8bea212b13e";
    private final static String SERVER = Utility.BOOTSTRAP_SERVERS;

    public static KafkaProducer createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536); //64 KB
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer producer = new KafkaProducer(props);
        return producer;
    }

    static void runProducer(int limit) throws Exception {
        final KafkaProducer producer = createProducer();
        try {
            for (int i = 1; i <= limit; i++) {

                ProducerRecord record = new ProducerRecord<>(TOPIC, String.valueOf(i), "Value-" + i);
                RecordMetadata metadata = (RecordMetadata) producer.send(record).get();
                System.out.printf("sent record(key=%s value=%s) meta(partition=%d, offset=%d) \n", record.key(),
                        record.value(), metadata.partition(), metadata.offset());
            }
        } finally {
            producer.flush();
            producer.close();
        }
    }

    public static void main(String[] args) throws Exception {
        runProducer(5);
    }

}
