FROM eclipse-temurin:21-jre

# The jar is built on the host by `./gradlew shadowJar` before this image is,
# so the image only has to run it.
COPY build/libs/app-all.jar /app.jar

# The base image would otherwise run everything as root. RocksDB needs somewhere
# writable for the state stores, and it must not be an OS temp directory -- Kafka
# Streams warns about those because the OS can clear them underneath a running
# app. Hence a dedicated directory owned by the same unprivileged user.
RUN useradd --system --uid 10001 --no-create-home app \
    && mkdir -p /var/lib/kafka-streams \
    && chown 10001:10001 /var/lib/kafka-streams
USER 10001

# The jar has no Main-Class, because Feeder and StreamsApp are both entry points
# sharing one jar -- hence -cp and an explicit class.
#
# Without -Xmx the JVM would take a quarter of the machine's memory as its
# maximum heap, and `--scale app=3` starts three of these.
ENTRYPOINT ["java", "-Xmx512m", "-cp", "/app.jar", "dev.kotryos.betengagement.streams.StreamsApp"]
