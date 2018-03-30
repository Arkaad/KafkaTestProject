package com.testcase.unused;

import com.testcase.util.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Created by palash on 2/22/2018.
 */
public class KafkaFileProducer {
    private static final String SERVER = KafkaConfig.BOOTSTRAP_SERVERS;
    private static final String TOPIC = "test";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536); //64 KB
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        KafkaProducer producer = new KafkaProducer(props);

        try {
            int i = 1;

//            File f = new File("D:\\Palash's Documents\\Dropbox\\eBooks\\arduinoGuide.pdf");
            File f = new File("C:\\Users\\palash\\Desktop\\test.txt");


            RecordMetadata metadata = null;
            try {
                FileInputStream fis = new FileInputStream(f);
                byte[] buff = new byte[1024 * 10];
                while (fis.read(buff) != -1) {

                    ProducerRecord<String, byte[]> record = new ProducerRecord<>(TOPIC, f.getName() + "@" + f.length(), buff);
                    metadata = (RecordMetadata) producer.send(record).get();

                    System.out.printf("sent record(key=%s value=%s) meta(partition=%d, offset=%d) \n", record.key(),
                            record.value(), metadata.partition(), metadata.offset());
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            producer.flush();
            producer.close();
        }
    }
}
