package dev.kotryos.betengagement.streams;

import dev.kotryos.betengagement.Config;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Owns the lifecycle: builds the topology, starts it, and shuts it down cleanly.
 * All of this is what Spring Boot would do for you in phase two.
 */
public final class StreamsApp {

    /** Names the consumer group and prefixes every internal topic, so changing it starts afresh. */
    public static final String APPLICATION_ID = "engagement-offers";

    private static final Logger log = LoggerFactory.getLogger(StreamsApp.class);

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        OfferTopology.build(builder);

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsProps());
        CountDownLatch stopped = new CountDownLatch(1);

        // Prints RUNNING -> REBALANCING -> RUNNING when an instance joins or dies.
        streams.setStateListener((newState, oldState) -> log.info("state {} -> {}", oldState, newState));

        // Without this a failed thread dies quietly and the app looks alive while idle.
        streams.setUncaughtExceptionHandler(error -> {
            log.error("uncaught exception, shutting this instance down", error);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            stopped.countDown();
        }));

        streams.start();
        log.info("started {} against {}", APPLICATION_ID, Config.bootstrapServers());

        try {
            stopped.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static Properties streamsProps() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Config.bootstrapServers());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, Config.schemaRegistryUrl());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        // A single bad record should not stop the app; log it and carry on.
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueExceptionHandler.class);

        // So an app started after the feeders still sees the bets they produced.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // The join task has two inputs: match events, and the re-keyed bets. With no
        // idling it drains whichever arrives first, and on a cold start that means every
        // match event meeting an empty table -- no offers, looking exactly like a bug.
        props.put(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, 5000L);

        // Relative to the repository root, which is where every documented command runs.
        // The container overrides it, since replicas must never share a state directory.
        props.put(StreamsConfig.STATE_DIR_CONFIG,
                System.getProperty("STATE_DIR", System.getenv().getOrDefault("STATE_DIR", "state/kafka-streams")));

        return props;
    }

    private StreamsApp() {
    }
}
