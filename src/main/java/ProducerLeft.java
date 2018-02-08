import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Created by Arka Dutta on 07-Feb-18.
 */
public class ProducerLeft {
    private final static String TOPIC = "TextLinesTopic";
    private final static String SERVER = "localhost:9092";

    public static KafkaProducer createProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", SERVER);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        KafkaProducer producer = new KafkaProducer(props);
        return producer;
    }

    static void runProducer(final int sendMessageCount) throws Exception {
        final KafkaProducer producer = createProducer();
        long time = System.currentTimeMillis();
        try {
//            for (long index = time; index < time + sendMessageCount; index++) {
//                final ProducerRecord record =
//                        new ProducerRecord<Long, String>(TOPIC, index,
//                                "Hello GIDs " + index);
//                RecordMetadata metadata = (RecordMetadata) producer.send(record).get();
//                long elapsedTime = System.currentTimeMillis() - time;
//                System.out.printf("sent record(key=%s value=%s) " +
//                                "meta(partition=%d, offset=%d) time=%d ms.\n",
//                        record.key(), record.value(), metadata.partition(),
//                        metadata.offset(), elapsedTime);
//            }
            for (int i = 1; i <= 10; i++) {
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
