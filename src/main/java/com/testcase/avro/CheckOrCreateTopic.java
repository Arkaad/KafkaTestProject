package com.testcase.avro;

import com.testcase.util.Utility;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by Arka on 02-Apr-18.
 */
public class CheckOrCreateTopic {
    private static KafkaProducer getProducer() throws IOException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Utility.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return new KafkaProducer(props);
    }

    private static byte[] getTopicValue() throws Exception {
        return AvroParser.getBinaryAvroData(getKey(), System.currentTimeMillis(), getKey(), 0, getKey(), getKey());
    }

    public static String getKey() {
        return "TOPIC-CHECK";
    }

    public static void checkOrCreateTopic() throws Exception {
        try (KafkaProducer producer = getProducer()) {
            ProducerRecord record = new ProducerRecord(Utility.KAFKA_TOPIC_LEFT, getKey(), getTopicValue());
            RecordMetadata metadata = (RecordMetadata) producer.send(record).get();
            System.out.println("Sent Record to Kafka Topic = " + metadata.topic() + " Key = " + record.key());
            record = new ProducerRecord(Utility.KAFKA_TOPIC_RIGHT, getKey(), getTopicValue());
            metadata = (RecordMetadata) producer.send(record).get();
            System.out.println("Sent Record to Kafka Topic = " + metadata.topic() + " Key = " + record.key());
            record = new ProducerRecord(Utility.KAFKA_TOPIC_DELTA, getKey(), getTopicValue());
            metadata = (RecordMetadata) producer.send(record).get();
            System.out.println("Sent Record to Kafka Topic = " + metadata.topic() + " Key = " + record.key());
            System.out.println("Topic Verified or Created Successfully ....");
        }
    }
}
