package dev.kotryos.betengagement.streams;

import dev.kotryos.betengagement.Config;
import dev.kotryos.betengagement.avro.Bet;
import dev.kotryos.betengagement.avro.MatchEvent;
import dev.kotryos.betengagement.avro.Offer;
import dev.kotryos.betengagement.avro.OpenBets;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A goal is scored. Find every player with an open bet on that match and, if the
 * campaign rules allow it, offer them a reward.
 */
public final class OfferTopology {

    public static final String BETS = "bets";
    public static final String MATCH_EVENTS = "match_events";
    public static final String OFFERS = "offers";

    /** Written by Debezium: the connector topic.prefix, then schema and table. */
    public static final String RULES = "db.public.engagement_rules";

    public static final String OPEN_BETS_STORE = "open-bets";

    private static final Logger log = LoggerFactory.getLogger(OfferTopology.class);

    public static KStream<String, Offer> build(StreamsBuilder builder) {
        Serde<String> keySerde = Serdes.String();
        SpecificAvroSerde<Bet> betSerde = specificSerde();
        SpecificAvroSerde<MatchEvent> matchEventSerde = specificSerde();
        SpecificAvroSerde<OpenBets> openBetsSerde = specificSerde();
        SpecificAvroSerde<Offer> offerSerde = specificSerde();
        GenericAvroSerde ruleSerde = genericSerde();

        // A table, so only the latest row per event_type counts and a delete arrives as
        // a tombstone -- an UPDATE in Postgres takes effect in a second, with no restart.
        // Global, so every instance holds all the rules and no candidate has to be
        // shuffled to reach one. Affordable only because reference data stays small.
        GlobalKTable<String, GenericRecord> rules =
                builder.globalTable(RULES, Consumed.with(keySerde, ruleSerde));

        // Bets arrive keyed by bet_id, but the question is "who is on this match", so
        // they are re-keyed and folded into one list per match. A windowed join cannot
        // replace this: bets are placed hours or days before kickoff.
        KTable<String, OpenBets> openBets = builder
                .stream(BETS, Consumed.with(keySerde, betSerde))
                .selectKey((betId, bet) -> bet.getEventId())
                .groupByKey(Grouped.with(keySerde, betSerde))
                .aggregate(
                        // Not OpenBets::new -- the Avro no-arg constructor leaves the list null.
                        () -> new OpenBets("", new ArrayList<>()),
                        OfferTopology::applyBet,
                        Materialized.<String, OpenBets, KeyValueStore<Bytes, byte[]>>as(OPEN_BETS_STORE)
                                .withKeySerde(keySerde)
                                .withValueSerde(openBetsSerde));

        KStream<String, Offer> offers = builder
                .stream(MATCH_EVENTS, Consumed.with(keySerde, matchEventSerde))
                // leftJoin in both places: an inner join would silently swallow the two
                // cases worth seeing -- a match with no bets, and a deleted rule.
                .leftJoin(openBets, OfferTopology::candidatesFor,
                        Joined.with(keySerde, matchEventSerde, openBetsSerde))
                // One match event becomes one candidate per open bet.
                .flatMapValues(candidates -> candidates)
                // No re-keying: a global table is looked up by whatever key this returns.
                .leftJoin(rules, (eventId, candidate) -> candidate.getEventType(),
                        OfferTopology::applyRule)
                .filter((eventId, offer) -> offer != null)
                // The sink upserts on player_id, so pk.mode=record_key needs it as the key.
                .selectKey((eventId, offer) -> offer.getPlayerId());

        offers.to(OFFERS, Produced.with(keySerde, offerSerde));
        return offers;
    }

    /** Adds a bet to its match, or removes it once settled, which is what bounds this state. */
    private static OpenBets applyBet(String eventId, Bet bet, OpenBets openBets) {
        // Copied, not edited in place: after a restore Avro hands the aggregate back as
        // an immutable array, so mutating it throws only once the app has restarted.
        List<Bet> bets = new ArrayList<>(openBets.getBets());
        bets.removeIf(held -> held.getBetId().equals(bet.getBetId()));

        if ("OPEN".equals(bet.getStatus())) {
            bets.add(bet);
        }

        return new OpenBets(eventId, bets);
    }

    /** Expands one match event into a candidate per open bet. A match with no bets is normal. */
    private static List<Offer> candidatesFor(MatchEvent event, OpenBets openBets) {
        if (openBets == null || openBets.getBets().isEmpty()) {
            log.info("{} {} -> no open bets", event.getEventId(), event.getEventType());
            return List.of();
        }

        List<Offer> candidates = new ArrayList<>();
        for (Bet bet : openBets.getBets()) {
            // Reward stays empty until applyRule knows which rule matched.
            candidates.add(new Offer(bet.getPlayerId(), bet.getBetId(), bet.getEventId(),
                    event.getEventType(), bet.getStake(), ""));
        }

        log.info("{} {} -> {} open bets", event.getEventId(), event.getEventType(), candidates.size());
        return candidates;
    }

    /** Rejects a candidate by returning null, logging why, so rule edits are visible in the console. */
    private static Offer applyRule(Offer candidate, GenericRecord rule) {
        if (rule == null) {
            log.info("dropped {} - no rule for {}", candidate.getPlayerId(), candidate.getEventType());
            return null;
        }
        if (!(Boolean) rule.get("active")) {
            log.info("dropped {} - rule {} is inactive", candidate.getPlayerId(), candidate.getEventType());
            return null;
        }

        // A double because the connector sets decimal.handling.mode=double, not bytes.
        double minStake = (Double) rule.get("min_stake");
        if (candidate.getStake() < minStake) {
            log.info("dropped {} - stake {} below {}", candidate.getPlayerId(), candidate.getStake(), minStake);
            return null;
        }

        // GenericRecord returns Avro Utf8, not String.
        candidate.setReward(rule.get("reward").toString());
        return candidate;
    }

    /** For records with a generated class; specific.avro.reader returns those types. */
    private static <T extends SpecificRecord> SpecificAvroSerde<T> specificSerde() {
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        serde.configure(Map.of(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, Config.schemaRegistryUrl(),
                KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true"), false);
        return serde;
    }

    /** For the rules, whose schema Debezium writes: there is no generated class to read into. */
    private static GenericAvroSerde genericSerde() {
        GenericAvroSerde serde = new GenericAvroSerde();
        serde.configure(Map.of(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, Config.schemaRegistryUrl()), false);
        return serde;
    }

    private OfferTopology() {
    }
}
