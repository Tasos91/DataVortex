-- Ο πίνακας στον οποίο γράφει ο consumer ό,τι διαβάζει από το Kafka.
--
-- TimescaleDB hypertable: ο πίνακας κόβεται αυτόματα σε chunks ανά ημέρα (event_time).
-- Τα παλιά chunks συμπιέζονται (columnstore), που είναι και ο λόγος που διαλέξαμε
-- TimescaleDB: ο όγκος είναι το βασικό μας πρόβλημα, όχι τα queries.
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS ingested_records
(
    -- Χρόνος του μηνύματος στο Kafka (CreateTime του producer), όχι η ώρα της εγγραφής:
    -- μένει ίδιος σε κάθε επανάληψη, ώστε το PRIMARY KEY να πιάνει τα διπλά.
    event_time      timestamptz      NOT NULL,
    topic           text             NOT NULL,
    kafka_partition integer          NOT NULL,
    kafka_offset    bigint           NOT NULL,
    source_id       text             NOT NULL,
    lat             double precision,
    lon             double precision,
    -- Τα δυναμικά πεδία του proto (map<string, Value>) όπως τα όρισε ο χρήστης στα fieldMappings
    fields          jsonb,
    -- Το αρχικό payload, για να μη χάνεται ό,τι δεν χαρτογραφήθηκε
    payload         jsonb,

    -- Ο consumer είναι at-least-once: σε κάθε rebalance ξαναδιαβάζονται μηνύματα.
    -- Το (topic, partition, offset) είναι μοναδικό στο Kafka, άρα με ON CONFLICT DO NOTHING
    -- η εγγραφή γίνεται idempotent χωρίς να χρειάζεται business key.
    -- Το event_time μπαίνει πρώτο επειδή το TimescaleDB απαιτεί τη διάσταση του χρόνου
    -- σε κάθε unique index.
    PRIMARY KEY (event_time, topic, kafka_partition, kafka_offset)
);

SELECT create_hypertable('ingested_records', by_range('event_time', INTERVAL '1 day'), if_not_exists => TRUE);

-- Μόνο ένα επιπλέον index: κάθε index κοστίζει σε κάθε insert.
CREATE INDEX IF NOT EXISTS idx_ingested_records_source_time
    ON ingested_records (source_id, event_time DESC);

-- Συμπίεση των chunks που δεν γράφονται πια. Το segmentby είναι μέσα στο PRIMARY KEY,
-- αλλιώς το TimescaleDB δεν επιτρέπει unique index σε συμπιεσμένο hypertable.
ALTER TABLE ingested_records
    SET (timescaledb.compress,
        timescaledb.compress_segmentby = 'topic',
        timescaledb.compress_orderby = 'event_time DESC');

SELECT add_compression_policy('ingested_records', INTERVAL '7 days', if_not_exists => TRUE);

-- ΠΡΟΣΟΧΗ: δεν βάζουμε retention policy. Αν τη θέλεις (σβήνει δεδομένα!), τρέξε χειροκίνητα:
--   SELECT add_retention_policy('ingested_records', INTERVAL '90 days');
