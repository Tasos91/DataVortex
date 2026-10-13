-- Γιατί υπάρχει αυτό το migration: το ερώτημα "δείξε μου τη χρονοσειρά ενός πεδίου" πάνω στο
-- jsonb θα σάρωνε όλες τις γραμμές του διαστήματος. Με συνεχή συγκεντρωτικά (continuous
-- aggregates) το κόστος γίνεται ανάλογο των ΚΟΥΒΑΔΩΝ που ζητάς, όχι του όγκου των δεδομένων.
--
-- Δομή:
--   metric_points   μία γραμμή ανά αριθμητικό πεδίο ανά μήνυμα (long format)
--   field_catalog   ποια πεδία υπάρχουν ανά πηγή (και τα κειμενικά), με πλήθη
--   metric_1m/1h/1d ιεραρχικά συγκεντρωτικά: το 1h χτίζεται από το 1m και το 1d από το 1h
--   records_1m      πλήθος μηνυμάτων ανά λεπτό/πηγή/topic

CREATE TABLE IF NOT EXISTS metric_points
(
    event_time timestamptz      NOT NULL,
    source_id  text             NOT NULL,
    topic      text             NOT NULL,
    field      text             NOT NULL,
    value      double precision NOT NULL
);
-- Χωρίς PRIMARY KEY επίτηδες: οι γραμμές παράγονται ΜΟΝΟ από όσες μπήκαν στο
-- ingested_records (RETURNING), που είναι ήδη idempotent. Έτσι γλιτώνουμε ένα φαρδύ
-- unique index σε κάθε insert, και το segmentby της συμπίεσης μένει ελεύθερο.
SELECT create_hypertable('metric_points', by_range('event_time', INTERVAL '1 day'), if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_metric_points_lookup ON metric_points (source_id, field, event_time DESC);
ALTER TABLE metric_points
    SET (timescaledb.compress,
        timescaledb.compress_segmentby = 'source_id, field',
        timescaledb.compress_orderby = 'event_time DESC');
SELECT add_compression_policy('metric_points', INTERVAL '7 days', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS field_catalog
(
    source_id  text        NOT NULL,
    topic      text        NOT NULL,
    field      text        NOT NULL,
    json_type  text        NOT NULL, -- number / string / boolean / array / object / null
    n          bigint      NOT NULL DEFAULT 0,
    first_seen timestamptz NOT NULL,
    last_seen  timestamptz NOT NULL,
    PRIMARY KEY (source_id, topic, field, json_type)
);

-- sum_v και sum_sq (όχι avg/stddev): έτσι το μέσο και η τυπική απόκλιση υπολογίζονται σε
-- ΟΠΟΙΟ επίπεδο, και τα συγκεντρωτικά αθροίζονται σωστά μεταξύ τους. Ο μέσος όρος μέσων
-- όρων θα ήταν λάθος όταν οι κουβάδες έχουν διαφορετικό πλήθος.
CREATE MATERIALIZED VIEW IF NOT EXISTS metric_1m
    WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT time_bucket(INTERVAL '1 minute', event_time) AS bucket,
       source_id, topic, field,
       count(*)::bigint AS n, sum(value) AS sum_v, sum(value * value) AS sum_sq,
       min(value) AS min_v, max(value) AS max_v
FROM metric_points
GROUP BY 1, 2, 3, 4;

CREATE MATERIALIZED VIEW IF NOT EXISTS metric_1h
    WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT time_bucket(INTERVAL '1 hour', bucket) AS bucket,
       source_id, topic, field,
       sum(n)::bigint AS n, sum(sum_v) AS sum_v, sum(sum_sq) AS sum_sq,
       min(min_v) AS min_v, max(max_v) AS max_v
FROM metric_1m
GROUP BY 1, 2, 3, 4;

CREATE MATERIALIZED VIEW IF NOT EXISTS metric_1d
    WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT time_bucket(INTERVAL '1 day', bucket) AS bucket,
       source_id, topic, field,
       sum(n)::bigint AS n, sum(sum_v) AS sum_v, sum(sum_sq) AS sum_sq,
       min(min_v) AS min_v, max(max_v) AS max_v
FROM metric_1h
GROUP BY 1, 2, 3, 4;

CREATE MATERIALIZED VIEW IF NOT EXISTS records_1m
    WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT time_bucket(INTERVAL '1 minute', event_time) AS bucket, source_id, topic, count(*)::bigint AS n
FROM ingested_records
GROUP BY 1, 2, 3;

-- Το materialized_only = false σημαίνει ότι τα φρέσκα δεδομένα φαίνονται ΑΜΕΣΩΣ (το
-- TimescaleDB ενώνει το υλικοποιημένο κομμάτι με ζωντανό query στην ουρά). Τα policies
-- υλικοποιούν σταδιακά, για να μένουν φτηνά τα queries.
SELECT add_continuous_aggregate_policy('metric_1m',
    start_offset => INTERVAL '3 hours', end_offset => INTERVAL '2 minutes',
    schedule_interval => INTERVAL '1 minute', if_not_exists => TRUE);
SELECT add_continuous_aggregate_policy('records_1m',
    start_offset => INTERVAL '3 hours', end_offset => INTERVAL '2 minutes',
    schedule_interval => INTERVAL '1 minute', if_not_exists => TRUE);
SELECT add_continuous_aggregate_policy('metric_1h',
    start_offset => INTERVAL '3 days', end_offset => INTERVAL '2 hours',
    schedule_interval => INTERVAL '10 minutes', if_not_exists => TRUE);
SELECT add_continuous_aggregate_policy('metric_1d',
    start_offset => INTERVAL '30 days', end_offset => INTERVAL '2 days',
    schedule_interval => INTERVAL '1 hour', if_not_exists => TRUE);
