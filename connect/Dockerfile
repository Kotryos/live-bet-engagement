FROM confluentinc/cp-kafka-connect:7.6.1

# Both plugin versions are pinned rather than ':latest'.
#
# This image runs Java 11, but Debezium 3.x is compiled for Java 17. Installing
# ':latest' puts the jars on disk where every class then fails to load, and
# Connect still starts and reports healthy with the connector silently absent.
# 2.5.4 is the Debezium release matching Confluent 7.6.
RUN confluent-hub install --no-prompt debezium/debezium-connector-postgresql:2.5.4

# Provides the JDBC sink and the bundled PostgreSQL driver it writes through.
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.9.6
