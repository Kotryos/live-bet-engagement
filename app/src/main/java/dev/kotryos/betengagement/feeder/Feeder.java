package dev.kotryos.betengagement.feeder;

import dev.kotryos.betengagement.Config;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

public final class Feeder {

    private static final Logger log = LoggerFactory.getLogger(Feeder.class);

    public static void main(String[] args) throws Exception {
        String topic = args[0];
        Path csv = Path.of(args[1]);
        long intervalMs = args.length > 2 ? Long.parseLong(args[2]) : 0;

        Schema schema = schemaOf(topic);
        String keyField = schema.getFields().get(0).name();
        int sent = 0;

        try (Producer<String, GenericRecord> producer = new KafkaProducer<>(producerProps());
             BufferedReader lines = Files.newBufferedReader(csv)) {

            lines.readLine();

            for (String line = lines.readLine(); line != null; line = lines.readLine()) {
                if (line.isBlank()) {
                    continue;
                }
                GenericRecord record = toRecord(line, schema);
                producer.send(new ProducerRecord<>(topic, record.get(keyField).toString(), record));
                log.info("{}", record);
                sent++;
                Thread.sleep(intervalMs);
            }
        }

        log.info("Sent {} records to {}", sent, topic);
    }

    private static Properties producerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, Config.schemaRegistryUrl());
        return props;
    }

    private static Schema schemaOf(String topic) throws Exception {
        try (InputStream avsc = Feeder.class.getResourceAsStream("/" + topic + ".avsc")) {
            return new Schema.Parser().parse(avsc);
        }
    }

    private static GenericRecord toRecord(String line, Schema schema) {
        String[] values = line.split(",");
        GenericRecord record = new GenericData.Record(schema);
        for (int i = 0; i < schema.getFields().size(); i++) {
            record.put(i, parse(values[i], schema.getFields().get(i).schema().getType()));
        }
        return record;
    }

    private static Object parse(String value, Schema.Type type) {
        return switch (type) {
            case INT -> Integer.parseInt(value);
            case LONG -> Long.parseLong(value);
            case DOUBLE -> Double.parseDouble(value);
            case FLOAT -> Float.parseFloat(value);
            case BOOLEAN -> Boolean.parseBoolean(value);
            default -> value;
        };
    }

    private Feeder() {
    }
}
