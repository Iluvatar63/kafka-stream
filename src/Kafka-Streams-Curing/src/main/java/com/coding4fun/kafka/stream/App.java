package com.coding4fun.kafka.stream;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Properties;

import javax.print.DocFlavor.STRING;

import com.coding4fun.kafka.models.*;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.streams.kstream.Produced;

import static java.util.Collections.singletonMap;

public class App 
{
    static final String END_OF_CURE_TOPIC_NAME="end-of-cure";
    static final String SHIFT_CHANGE_TOPIC_NAME="shift-changed";
    static final String ENHANCED_SHIFT_CHANGE_TOPIC_NAME="enhanced-end-of-cure";
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_SCHEMA_REGISTRY_URL = "http://localhost:8081";

    public static void main( String[] args )
    {
        Map<String, String>  serdeConfig = singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, DEFAULT_SCHEMA_REGISTRY_URL);
        final Serde <String> stringSerde = Serdes.String();
        final Serde<EndOfCure> enfOfCureSerializer  = new SpecificAvroSerde<>();
        enfOfCureSerializer.configure(serdeConfig, false);
        final Serde<ShiftChanged> shiftChangeSerializer  = new SpecificAvroSerde<>();
        shiftChangeSerializer.configure(serdeConfig, false);
        final Serde<EnhancedEndOfCure> enhancedEnfOfCureSerializer  = new SpecificAvroSerde<>();
        enhancedEnfOfCureSerializer.configure(serdeConfig, false);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String,EndOfCure> endOfCure = builder.stream(END_OF_CURE_TOPIC_NAME, Consumed.with(stringSerde, enfOfCureSerializer));
        KTable<String, ShiftChanged> shiftChangeTable = builder.table(SHIFT_CHANGE_TOPIC_NAME, Consumed.with(stringSerde, shiftChangeSerializer));
        KStream<String,EnhancedEndOfCure> enhancedEndOfCure  = endOfCure.join(shiftChangeTable, new EndOfCureJoiner());
        enhancedEndOfCure.to(ENHANCED_SHIFT_CHANGE_TOPIC_NAME, Produced.with(stringSerde, enhancedEnfOfCureSerializer));
        Topology topology = builder.build();

        final KafkaStreams streams = new KafkaStreams(topology, getProperties());
        streams.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
            System.out.println(String.format("Something bad happened in thread {}: {}", thread, throwable.getMessage()));
            streams.close(Duration.of(3, ChronoUnit.SECONDS));
            System.exit(1);
        });
        streams.setStateListener((before, after) -> System.out.println(String.format("Switching from state {} to {}", before, after)));
        streams.start();
    }

    private static Properties getProperties() {
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "coding4fun-stream");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVERS);
        settings.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, DEFAULT_SCHEMA_REGISTRY_URL);
        return settings;
    }
}
