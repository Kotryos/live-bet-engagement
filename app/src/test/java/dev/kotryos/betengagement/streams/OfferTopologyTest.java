package dev.kotryos.betengagement.streams;

import dev.kotryos.betengagement.avro.Bet;
import dev.kotryos.betengagement.avro.MatchEvent;
import dev.kotryos.betengagement.avro.Offer;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

class OfferTopologyTest {

    private static final String REGISTRY_SCOPE = "offer-topology-test";
    private static final String REGISTRY_URL = "mock://" + REGISTRY_SCOPE;

    private static final Schema RULE_SCHEMA = new Schema.Parser().parse("""
            {
              "type": "record",
              "name": "Value",
              "namespace": "db.public.engagement_rules",
              "fields": [
                {"name": "event_type", "type": "string"},
                {"name": "min_stake", "type": "double"},
                {"name": "reward", "type": "string"},
                {"name": "active", "type": "boolean"}
              ]
            }
            """);

    private TopologyTestDriver driver;
    private TestInputTopic<String, Bet> bets;
    private TestInputTopic<String, MatchEvent> matchEvents;
    private TestInputTopic<String, GenericRecord> rules;
    private TestOutputTopic<String, Offer> offers;

    @BeforeEach
    void setUp(@TempDir Path stateDir) {
        System.setProperty("SCHEMA_REGISTRY_URL", REGISTRY_URL);

        StreamsBuilder builder = new StreamsBuilder();
        OfferTopology.build(builder);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, REGISTRY_SCOPE);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());

        driver = new TopologyTestDriver(builder.build(), props);

        bets = driver.createInputTopic(OfferTopology.BETS,
                Serdes.String().serializer(), OfferTopologyTest.<Bet>specific().serializer());
        matchEvents = driver.createInputTopic(OfferTopology.MATCH_EVENTS,
                Serdes.String().serializer(), OfferTopologyTest.<MatchEvent>specific().serializer());
        rules = driver.createInputTopic(OfferTopology.RULES,
                Serdes.String().serializer(), generic().serializer());
        offers = driver.createOutputTopic(OfferTopology.OFFERS,
                Serdes.String().deserializer(), OfferTopologyTest.<Offer>specific().deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
        MockSchemaRegistry.dropScope(REGISTRY_SCOPE);
        System.clearProperty("SCHEMA_REGISTRY_URL");
    }

    @Test
    void matchEvent_twoOpenBetsAndActiveRule_producesOneOfferPerBet() {
        // given
        givenRule("GOAL", 5.00, "free_bet_5", true);
        givenBet("bet-1", "player-1", "match-1", 10.00, "OPEN");
        givenBet("bet-2", "player-2", "match-1", 20.00, "OPEN");

        // when
        whenGoalIsScored("match-1");

        // then
        assertThat(offers.readKeyValuesToList())
                .extracting(keyValue -> keyValue.key, keyValue -> keyValue.value.getReward())
                .containsExactlyInAnyOrder(
                        tuple("player-1", "free_bet_5"),
                        tuple("player-2", "free_bet_5"));
    }

    @Test
    void matchEvent_stakeBelowMinStake_omitsThatBet() {
        // given
        givenRule("GOAL", 25.00, "free_bet_5", true);
        givenBet("bet-1", "player-1", "match-1", 10.00, "OPEN");
        givenBet("bet-2", "player-2", "match-1", 30.00, "OPEN");

        // when
        whenGoalIsScored("match-1");

        // then
        assertThat(offers.readKeyValuesToList())
                .extracting(keyValue -> keyValue.key)
                .containsExactly("player-2");
    }

    @Test
    void matchEvent_inactiveRule_producesNoOffers() {
        // given
        givenRule("GOAL", 5.00, "free_bet_5", false);
        givenBet("bet-1", "player-1", "match-1", 10.00, "OPEN");
        givenBet("bet-2", "player-2", "match-1", 20.00, "OPEN");

        // when
        whenGoalIsScored("match-1");

        // then
        assertThat(offers.isEmpty()).isTrue();
    }

    @Test
    void matchEvent_betAlreadySettled_omitsThatBet() {
        // given
        givenRule("GOAL", 5.00, "free_bet_5", true);
        givenBet("bet-1", "player-1", "match-1", 10.00, "OPEN");
        givenBet("bet-2", "player-2", "match-1", 20.00, "OPEN");
        whenGoalIsScored("match-1");
        drainOffers();
        givenBet("bet-1", "player-1", "match-1", 10.00, "SETTLED");

        // when
        whenGoalIsScored("match-1");

        // then
        assertThat(offers.readKeyValuesToList())
                .extracting(keyValue -> keyValue.key)
                .containsExactly("player-2");
    }

    @Test
    void matchEvent_ruleRaisedAfterEarlierEvent_appliesNewRule() {
        // given
        givenRule("GOAL", 5.00, "free_bet_5", true);
        givenBet("bet-1", "player-1", "match-1", 10.00, "OPEN");
        whenGoalIsScored("match-1");
        drainOffers();
        givenRule("GOAL", 50.00, "free_bet_5", true);

        // when
        whenGoalIsScored("match-1");

        // then
        assertThat(offers.isEmpty()).isTrue();
    }

    private void givenRule(String eventType, double minStake, String reward, boolean active) {
        GenericRecord rule = new GenericData.Record(RULE_SCHEMA);
        rule.put("event_type", eventType);
        rule.put("min_stake", minStake);
        rule.put("reward", reward);
        rule.put("active", active);
        rules.pipeInput(eventType, rule);
    }

    private void givenBet(String betId, String playerId, String eventId, double stake, String status) {
        bets.pipeInput(betId, new Bet(betId, playerId, eventId, "match_winner", stake, status));
    }

    private void whenGoalIsScored(String eventId) {
        matchEvents.pipeInput(eventId, new MatchEvent(eventId, 23, "GOAL", 1, 0, 0L));
    }

    private void drainOffers() {
        offers.readKeyValuesToList();
    }

    private static <T extends SpecificRecord> SpecificAvroSerde<T> specific() {
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        serde.configure(Map.of(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, REGISTRY_URL,
                KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true"), false);
        return serde;
    }

    private static GenericAvroSerde generic() {
        GenericAvroSerde serde = new GenericAvroSerde();
        serde.configure(Map.of(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, REGISTRY_URL), false);
        return serde;
    }
}
