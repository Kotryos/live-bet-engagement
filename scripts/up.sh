#!/usr/bin/env bash
#
# Brings the whole stack up from nothing. Safe to run repeatedly.
#
# Nothing here declares a volume, so `docker compose down` already discards every
# topic, schema and connector. That is deliberate: each run starts from a known
# state, and this script rebuilds everything it needs.

set -euo pipefail

cd "$(dirname "$0")/.."

CONNECT=localhost:8083
CONNECTORS=(rules-source offers-sink)

echo "==> infrastructure"
docker compose up -d --wait

echo "==> source topics"
# Kafka Streams refuses to create its own source topics and dies at startup with
# MissingSourceTopicException if they are absent. Creating them up front means the
# app and the feeders can be started in either order.
for topic in bets match_events; do
    docker compose exec -T kafka kafka-topics \
        --bootstrap-server localhost:29092 \
        --create --if-not-exists --topic "$topic" \
        --partitions 3 --replication-factor 1 >/dev/null
    echo "    $topic"
done

echo "==> connectors"
# Deleted first so that re-running this script replaces the connectors rather
# than failing on a name that already exists.
for connector in "${CONNECTORS[@]}"; do
    curl -sS -X DELETE "$CONNECT/connectors/$connector" >/dev/null 2>&1 || true
    curl -sS -X POST -H 'Content-Type: application/json' \
        --data @"connectors/$connector.json" "$CONNECT/connectors" >/dev/null
done

for connector in "${CONNECTORS[@]}"; do
    until [ "$(curl -s "$CONNECT/connectors/$connector/status" | jq -r '.connector.state')" = RUNNING ]; do
        sleep 2
    done
    echo "    $connector RUNNING"
done

echo "==> app"
./gradlew -q :app:shadowJar
docker compose --profile full up -d --build --scale app=1

cat <<'EOF'

Ready. Feed it with:

  java -cp app/build/libs/app-all.jar dev.kotryos.betengagement.feeder.Feeder bets data/bets.csv
  java -cp app/build/libs/app-all.jar dev.kotryos.betengagement.feeder.Feeder match_events data/match-events.csv 4000

Then watch:

  docker compose logs -f app
  docker exec postgres psql -U postgres -d engagement -c 'SELECT * FROM offers;'
EOF
