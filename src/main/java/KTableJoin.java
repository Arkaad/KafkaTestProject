import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

/**
 * Created by Arka Dutta on 08-Feb-18.
 */
public class KTableJoin {

    public static void main(String[] args) throws Exception {

        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG,
                "table-join-kafka-streams");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");
        config.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
//        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        KStreamBuilder builder = new KStreamBuilder();

        KTable left = builder.table("TextLinesTopic", "Table1");
        KTable right = builder.table("RekeyedIntermediateTopic", "Table2");
        KTable joined = left.join(right,
                (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue);
        joined.toStream().to("WordsWithCountsTopic");

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));


    }
}
