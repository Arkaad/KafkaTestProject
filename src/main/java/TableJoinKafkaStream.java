import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

/**
 * Created by Arka Dutta on 08-Feb-18.
 */
public class TableJoinKafkaStream {

    public static void main(String[] args) throws Exception {

        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG,
                "table-join-kafka-streams");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");
        config.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());
        config.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());

        KStreamBuilder builder = new KStreamBuilder();

        final Serde<String> stringSerde = Serdes.String();
        KStream left = builder.stream(TopologyBuilder.AutoOffsetReset.LATEST, stringSerde, stringSerde, "TextLinesTopic");
        KStream right = builder.stream(TopologyBuilder.AutoOffsetReset.LATEST, stringSerde, stringSerde, "RekeyedIntermediateTopic");
        KStream joined = left.outerJoin(right,
                new ValueJoiner() {
                    @Override
                    public Object apply(Object leftValue, Object rightValue) {
                        return "left=" + leftValue + ", right=" + rightValue;
//                        if (leftValue == null && rightValue != null)
//                            return "left=" + leftValue + ", right=" + rightValue;
//                        else
//                            return null;
                    }
                }, /* ValueJoiner */
                JoinWindows.of(30 * 1000L)
        );
        joined.to(stringSerde, stringSerde, "WordsWithCountsTopic");
        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));


    }
}
