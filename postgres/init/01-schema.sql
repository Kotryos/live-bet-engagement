-- Runs once, when the Postgres data volume is created fresh.
-- To re-run it after editing: docker compose down -v && docker compose up -d


-- The rules this service owns and operators edit. Debezium captures changes here and
-- the Streams app keeps them in a KTable, so an UPDATE takes effect in the
-- running pipeline within a second and without a restart.
CREATE TABLE engagement_rules (
    event_type  TEXT PRIMARY KEY,
    min_stake   NUMERIC(10,2) NOT NULL,
    reward      TEXT NOT NULL,
    active      BOOLEAN NOT NULL DEFAULT true
);

-- By default Postgres logs only the primary key of a changed row; FULL makes it
-- log the whole previous row too.
--
-- Nothing here requires it. event_type is the primary key, so a delete already
-- carries the key the KTable needs to drop the row, and the transform chain in
-- rules-source.json keeps only the "after" image regardless. It is kept because
-- it costs nothing at three rows, and because FULL is mandatory on a table with
-- no primary key -- there Postgres refuses to update or delete a published row.
ALTER TABLE engagement_rules REPLICA IDENTITY FULL;

INSERT INTO engagement_rules (event_type, min_stake, reward, active) VALUES
  ('GOAL',     10.00, 'free_bet_5',    true),
  ('RED_CARD', 25.00, 'cashout_offer', true),
  ('HALF_TIME',50.00, 'free_bet_10',   true);


-- Where the JDBC sink writes the pipeline's output: the reward a player has
-- qualified for, and the event and bet that earned it. Keyed by player_id and
-- written as an upsert, so it holds the latest offer per player and a plain
-- SELECT reads cleanly at any moment.
--
-- updated_at is set when a player's row first appears and is not refreshed by
-- later upserts, because the sink only writes the columns the Offer record
-- carries. Read it as first-seen, not last-changed.
CREATE TABLE offers (
    player_id    TEXT PRIMARY KEY,
    bet_id       TEXT,
    event_id     TEXT,
    event_type   TEXT,
    stake        DOUBLE PRECISION,
    reward       TEXT,
    updated_at   TIMESTAMP DEFAULT now()
);


-- Postgres keeps ONE write-ahead log for the whole server, covering every table.
-- A publication is the filter that decides whose changes get decoded out of it.
--
-- It names engagement_rules only, so the sink's own writes to offers are never
-- decoded and fed back in. rules-source.json filters again with
-- table.include.list, but this one stops the data before it crosses the wire.
--
-- Creating it here also keeps the connector from needing permission to make one
-- itself, which it would otherwise do as FOR ALL TABLES.
CREATE PUBLICATION engagement_pub FOR TABLE engagement_rules;
