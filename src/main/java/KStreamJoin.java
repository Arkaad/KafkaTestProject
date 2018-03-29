import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Created by Arka Dutta on 08-Feb-18.
 */
public class KStreamJoin {

    public static void main(String[] args) {

        Properties streamsConfiguration = new Properties();
//        streamsConfiguration.put("application.id", "wordcount-lambda-example");
        streamsConfiguration.put("bootstrap.servers", "localhost:9092");
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-kafka-project-app");

        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> textLines = builder.stream(stringSerde, stringSerde, "kafka-test-left");
        Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);

//        KStream<String, Long> wordCounts = textLines
//                .flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
//                .map(((key, value) -> new KeyValue<String, String>(value, value)))
//                .through("kafka-test-right")
//                .countByKey("Counts").toStream();
//
//        wordCounts.to(stringSerde, longSerde, "kafka-test-result");
        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        System.out.println("starting kafka streams...");
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
