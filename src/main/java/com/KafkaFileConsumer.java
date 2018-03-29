package com;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

/**
 * Created by Arka Dutta on 07-Feb-18.
 */
public class KafkaFileConsumer {
    private final static String TOPIC = "test";
    private final static String SERVER = "localhost:9092";

    private static KafkaConsumer createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                SERVER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "KafkaFileConsumer");
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

        File f = null;
        FileOutputStream fout = null;
        long lenCount = 0;
        long size = -1;
        while (true) {
            final ConsumerRecords<String, byte[]> consumerRecords =
                    consumer.poll(1000);
            for (ConsumerRecord<String, byte[]> record : consumerRecords) {
                if (record.key() != null && record.key().contains("@") && record.value() != null) {
                    if (f == null) {
                        String key = record.key();
                        String[] split = key.split("@");
                        String fileName = split[0];
                        size = Long.parseLong(split[1]);
                        System.out.println("fileName = " + fileName + ", size = " + size);
                        f = new File(fileName);
                        fout = new FileOutputStream(f);
                        lenCount = 0;
                        lenCount += record.value().length;
                        fout.write(record.value());
                        System.out.println("Written " + lenCount);
                        fout.flush();
                    } else {
                        lenCount += record.value().length;
                        if (fout != null) {
                            fout.write(record.value());
                            System.out.println("Written " + lenCount);
                            fout.flush();
                        }
                    }
                }
            }
            if (fout != null && (lenCount >= size)) {
                fout.flush();
                fout.close();
                System.out.println("file written");
                fout = null;
                f=null;
            }
            consumer.commitAsync();
        }
    }

    public static void main(String... args) throws Exception {
        runConsumer();
    }
}
