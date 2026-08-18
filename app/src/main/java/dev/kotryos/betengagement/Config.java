package dev.kotryos.betengagement;

public final class Config {

    public static String bootstrapServers() {
        return setting("KAFKA_BOOTSTRAP", "localhost:9092");
    }

    public static String schemaRegistryUrl() {
        return setting("SCHEMA_REGISTRY_URL", "http://localhost:8081");
    }

    private static String setting(String name, String fallback) {
        String value = System.getProperty(name, System.getenv(name));
        return (value == null || value.isBlank()) ? fallback : value;
    }

    private Config() {
    }
}
