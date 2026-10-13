# datavortex

Spring Boot 4 / Java 21 εφαρμογή (διπλωματική): δέχεται δεδομένα μέσω REST, τα μετατρέπει σε
Protobuf και τα στέλνει στο Kafka· ένας batch consumer τα διαβάζει πίσω και τα αποθηκεύει σε
TimescaleDB (και τροφοδοτεί ένα απλό dashboard).
Η υπόλοιπη περιγραφή εδώ είναι μόνο ό,τι δεν φαίνεται εύκολα από τον κώδικα.

## Εντολές (το μηχάνημα έχει αργή CPU, 4 πυρήνες: τρέχε το ελάχιστο)
- Η default `java` είναι 11. Πάντα: `export JAVA_HOME=~/.jdks/corretto-21.0.2`
- Compile: `./mvnw -o -q compile`  (offline, χωρίς `clean`)
- Ένα test: `./mvnw -o -q test -Dtest=RecordMapperTest`
- ΜΗΝ τρέχεις `KafkaCollectorApplicationTests` (`@SpringBootTest`): θέλει Postgres + Kafka και είναι αργό.
- Μην τρέχεις όλο το test suite ή `package`/`clean` αν δεν σου το ζητήσουν.
- Το Protobuf class (`IngestedRecordProto`) παράγεται στο compile από `src/main/proto/`· μην το επεξεργάζεσαι χειροκίνητα
  και μην ψάχνεις το πηγαίο του στο `src/`.

## Δομή (`com.thesis.kafkacollector`)
- `controller/IngestionController`:
  - `POST /api/v1/upload` — JSON με `payload` μέσα, ή `fileType` + `filePath`. **Όλο το σώμα μπαίνει στη μνήμη**
    (και σε αντίγραφα: Jackson bind -> JsonNode -> JSON string ανά record). Για βολικά, μικρά-μεσαία φορτία.
  - `POST /api/v1/stream` — **NDJSON** για μεγάλο όγκο: πρώτη γραμμή οι ρυθμίσεις, μετά μία εγγραφή ανά γραμμή.
    Στη μνήμη βρίσκεται μία γραμμή τη φορά· μια χαλασμένη γραμμή προσπερνιέται και μετριέται.
  - Και τα δύο: `200` μετά το `flush()` του producer (άρα το 200 σημαίνει "είναι στο Kafka"), `207` αν κάποια
    μηνύματα απέτυχαν, `400` για λάθος request, `429` όταν τρέχουν ήδη `datavortex.max-concurrent-ingests`
    εισαγωγές, `503` όταν ο broker δεν απαντά.
- `controller/DashboardController`: `/dashboard` (HTML) και `/dashboard/api/stats`.
- `service/FileStreamingService`: CSV, JSON αρχείο και inline `payload` → Protobuf → Kafka.
- `service/KafkaTopicService`: ελέγχει/δημιουργεί το topic του request (fail-fast, με cache).
- `service/IngestedRecordWriter`: γράφει το batch στη βάση με ΕΝΑ multi-row INSERT
  (`ON CONFLICT DO NOTHING`), με fallback μία-μία γραμμή όταν αποτύχει το batch.
- `mapping/JsonPathResolver` και `mapping/RecordMapper`: εφαρμόζουν τα `fieldMappings` στο payload.
- `config/KafkaConsumerConfig`: bean `Consumer<Message<List<Object>>> consumeData` — **batch** consumer
  (Spring Cloud Stream) → `IngestedRecordWriter`. Τα Kafka metadata (topic/partition/offset/timestamp)
  έρχονται ως Lists στα headers.
- `config/KafkaErrorHandlingConfig`: retries και DLQ. Το DLQ `KafkaTemplate` ΔΕΝ είναι bean επίτηδες — ένα δεύτερο
  `KafkaTemplate`/`ProducerFactory` στο context κάνει το KafkaAutoConfiguration να μην φτιάξει το κανονικό.
- `service/KafkaConsumerService`: ολόκληρο σε σχόλια, νεκρός κώδικας· μην τον ξαναενεργοποιείς χωρίς λόγο.
- `dto/IngestionRequest`: `sourceId`, `fileType`, `filePath`, `topic`, `createTopicIfMissing`, `partitions`,
  `keyField`, `payload`, `fieldMappings`.

## Βασικοί κανόνες σχεδιασμού
- Το μήνυμα στο Kafka είναι **Protobuf μέσω Schema Registry** (`KafkaProtobufSerializer`/`KafkaProtobufDeserializer`,
  `SCHEMA_REGISTRY_URL`, default `http://localhost:8081`). Producer: `KafkaTemplate<String, IngestedRecord>`,
  consumer: `Consumer<IngestedRecord>` (δεν γίνεται χειροκίνητο `parseFrom`). Δεν υπάρχει Avro στο project.
- Αν αλλάξεις το `.proto` ασύμβατα, ο registry θα απορρίψει το νέο schema (έλεγχος συμβατότητας).
- Παλιά μηνύματα στο topic που στάλθηκαν ως raw bytes (χωρίς schema id) δεν διαβάζονται από τον consumer: χρησιμοποίησε νέο topic ή άδειασέ το.
- Τα δυναμικά πεδία ζουν στο `map<string, google.protobuf.Value> fields = 5` του
  `src/main/proto/ingested_record.proto`. Προσθήκη πεδίου = νέος αριθμός, ποτέ μην αλλάζεις/ξαναχρησιμοποιείς υπάρχοντες.
- `fieldMappings`: `{"ΟνομαΠεδίου": "$.διαδρομή.στο.json"}`. Τα ονόματα `lat`/`lon` γεμίζουν τα σταθερά πεδία, όλα τα άλλα
  πάνε στο `fields`. Σκέτο όνομα (χωρίς `$`) = κλειδί πρώτου επιπέδου (CSV). Δεν βρέθηκε τιμή → παράλειψη + warn, όχι σφάλμα.
- **Topic ανά request:** `"topic": "..."` στο request· αν λείπει, το default του `application.yaml`. Αν το topic δεν
  υπάρχει → 400 αμέσως (ο broker έχει `auto.create.topics.enable=false`, αλλιώς θα το έφτιαχνε με 1 partition).
  Με `"createTopicIfMissing": true` το φτιάχνει η εφαρμογή (`partitions`, default 6).
- **Partition key:** `"keyField": "$.vessel.mmsi"`. Ίδιο κλειδί → ίδιο partition → εγγυημένη σειρά για την οντότητα.
  Χωρίς `keyField` το κλειδί είναι `null` και τα μηνύματα μοιράζονται (μέγιστο throughput, καμία εγγύηση σειράς).
  ΜΗΝ βάλεις ξανά το `sourceId` ως κλειδί: στέλνει τα πάντα σε ένα partition και μηδενίζει τον παραλληλισμό.
- **Ο consumer ακούει σε pattern**, όχι σε ένα topic: `destination: 'ingest\..*'` με `destination-is-pattern: true`.
  Άρα τα topics του χρήστη πρέπει να λέγονται `ingest.<κάτι>` για να διαβαστούν (αλλιώς μόνο warning στο log).
  Το DLQ (`dlq.ingest`) είναι επίτηδες έξω από το pattern, αλλιώς θα το ξαναδιάβαζε σε βρόχο.
- **Batch mode:** `batch-mode: true`, `max.poll.records: 500`, `fetch.min.bytes: 65536`, `fetch.max.wait.ms: 500`.
  Με `fetch.min.bytes: 1` τα batches έβγαιναν ~45 μηνύματα· τώρα φτάνουν το όριο των 500.
  `concurrency` (default 3) <= partitions, αλλιώς τα παραπανίσια threads μένουν άεργα.
- **Poison pill:** `ErrorHandlingDeserializer` + `DefaultErrorHandler` + `DeadLetterPublishingRecoverer`. Χαλασμένο
  μήνυμα → `dlq.ingest` αυτούσιο (με headers το αρχικό topic/partition/offset) και ο consumer συνεχίζει.
- **Ο τύπος του consumer είναι `List<Object>`, όχι `List<IngestedRecord>`.** Ένα μήνυμα που δεν διαβάστηκε φτάνει
  ως `KafkaNull`· με δηλωμένο τύπο record σκάει `ClassCastException` και φεύγει ΟΛΟ το batch (500 καλά μηνύματα)
  στο DLQ. Αντ' αυτού γράφουμε τα καλά και πετάμε `BatchListenerFailedException(msg, index)` για το ένα χαλασμένο.
- **Αν πέσει η βάση δεν πάει τίποτα στο DLQ:** ο `setBackOffFunction` δίνει άπειρα retries στα σφάλματα
  υποδομής (`CannotGetJdbcConnectionException`, `DataAccessResourceFailureException`, ...). Τα offsets δεν
  γίνονται commit, το Kafka κρατά τα δεδομένα, και η ροή συνεχίζει μόλις επανέλθει η βάση.
- **Η απάντηση περιμένει τα acks** (`kafkaTemplate.flush()` στο `finish()`). Πριν, η απάντηση έλεγε
  "στάλθηκαν N" ενώ τα μηνύματα ήταν ακόμα στο buffer του producer: αν έπεφτε το Kafka, ο client είχε 200
  και τα δεδομένα δεν υπήρχαν πουθενά. ΜΗΝ αφαιρέσεις το flush.
- **Backpressure:** semaphore `datavortex.max-concurrent-ingests` (default 4) και `max.block.ms: 10000` στον
  producer. Χωρίς αυτά, αρκετά ταυτόχρονα requests γεμίζουν το `buffer-memory` και μπλοκάρουν όλα τα Tomcat
  threads μέσα στο `send()` για 60s.
- Στο Spring Boot 4 το MVC χρησιμοποιεί Jackson 3, ενώ ο κώδικας χρησιμοποιεί `com.fasterxml` (Jackson 2). Γι' αυτό το
  `payload` στο DTO είναι `Object` και όχι `JsonNode`· μην το αλλάξεις.

## Βάση δεδομένων
- **TimescaleDB** (`timescale/timescaledb:2.30.2-pg17`), δηλαδή PostgreSQL + extension χρονοσειρών.
  Ίδιο SQL, ίδιος driver. Volume `tsdata`· το παλιό `pgdata` (postgres:15) μένει άθικτο και δεν χρησιμοποιείται.
- **ΧΩΡΙΣ JPA/Hibernate.** Δεν υπάρχουν entities· γράφουμε με `JdbcTemplate` σε batches. Λόγος: το Hibernate
  απενεργοποιεί το JDBC batching με `@GeneratedValue(IDENTITY)`, που σημαίνει ένα INSERT ανά record.
  Αν χρειαστεί JPA για μικρούς πίνακες, πρόσθεσε πίσω το `spring-boot-starter-data-jpa`.
- **Το schema το φτιάχνει το Flyway** (`src/main/resources/db/migration/`), όχι το `ddl-auto` (είναι `none`).
  Στο Spring Boot 4 χρειάζεται το `spring-boot-starter-flyway`: μόνο με το `flyway-core` δεν τρέχει τίποτα.
  Νέα αλλαγή schema = νέο αρχείο `V2__...sql`· ΜΗΝ επεξεργάζεσαι migration που έχει τρέξει.
- Πίνακας `ingested_records`: hypertable με διάσταση χρόνου `event_time` (chunks ανά ημέρα), συμπίεση μετά
  από 7 ημέρες (`segmentby = topic`), και `PRIMARY KEY (event_time, topic, kafka_partition, kafka_offset)`.
- **Idempotency:** ο consumer είναι at-least-once, άρα τα διπλά είναι αναμενόμενα. Το `(topic, partition, offset)`
  του Kafka είναι μοναδικό, οπότε `INSERT ... ON CONFLICT DO NOTHING` αρκεί — δοκιμασμένο ότι δουλεύει και σε
  συμπιεσμένο chunk. Το `event_time` ΠΡΕΠΕΙ να είναι το timestamp του μηνύματος Kafka (σταθερό σε κάθε
  επανάληψη), όχι `now()`, αλλιώς το dedup δεν πιάνει.
- **Γράψιμο:** ένα multi-row `INSERT` ανά batch (`datavortex.batch-rows`, default 1000 γραμμές/statement) και
  `reWriteBatchedInserts=true` στο JDBC URL. Αν το batch αποτύχει, ξαναδοκιμάζεται μία-μία γραμμή ώστε να
  περάσουν οι υπόλοιπες· μια γραμμή που απορρίπτεται από τη βάση λογίζεται ως ERROR με topic/partition/offset.
- `lat`/`lon` είναι `optional` στο proto: αν ο χρήστης δεν τα χαρτογραφήσει, στη βάση μπαίνει NULL και όχι 0.0.
  Γι' αυτό ο writer χρησιμοποιεί `hasLat()`/`hasLon()`.
- Το TimescaleDB απαιτεί τη διάσταση χρόνου σε κάθε unique index, και τα `segmentby` columns πρέπει να είναι
  μέσα στο unique index· γι' αυτό το `segmentby` είναι μόνο `topic` και όχι `topic, source_id`.
- ΔΕΝ υπάρχει retention policy (θα έσβηνε δεδομένα). Οδηγία για το πώς μπαίνει: στο V1 migration.

## Ρυθμίσεις και υποδομή
- `application.yaml`: port 8080, topic `ais-data` (`KAFKA_TOPIC`), brokers `localhost:9092`, Postgres `localhost:5432/bigdata_db`.
  Όλα με env overrides.
- `docker-compose.yml` (στη ρίζα): timescaledb (service `postgres`), kafka (cp 7.6.0, KRaft), schema-registry (:8081, ΑΠΑΡΑΙΤΗΤΟ για producer και consumer), prometheus, grafana.
  Δεν έχει όρια μνήμης. Ξεκίνα μόνο ό,τι χρειάζεται: `docker compose up -d postgres kafka schema-registry`.
  Η εφαρμογή ΔΕΝ ξεκινά χωρίς τη βάση (το Flyway τρέχει στο startup).
- Τα topics ΔΕΝ δημιουργούνται αυτόματα, ούτε από τον broker ούτε από τον binder. Είτε φτιάξ' το με
  `kafka-topics --create`, είτε στείλε `createTopicIfMissing: true`.

## Μετρημένες αποδόσεις (i3-8100, 4 πυρήνες, όλα στο ίδιο μηχάνημα)
- 100.000 μηνύματα: publish 8,5s· αποθήκευση ολοκληρωμένη σε 12s συνολικά (~11.400 rows/s).
- Μέσο batch ~467 μηνύματα, 214 batches.
- Πλήρες replay του topic: 0 νέες γραμμές (120.003 πριν και μετά), 121.386 διπλά απορρίφθηκαν από τη βάση.
- Βάση κάτω για 25s: 0 μηνύματα στο DLQ, όλα γράφτηκαν μόλις επανήλθε.
- NDJSON 200.000 εγγραφές (25MB): 11,5s το publish, 31s μέχρι να είναι όλα στη βάση, και η μνήμη της JVM
  ανέβηκε μόνο 85MB (548 -> 634MB).
- 8 ταυτόχρονα requests με όριο 4: 4 x 200 και 4 x 429.

## Στυλ
- Σχόλια και μηνύματα log στα ελληνικά (όπως ο υπάρχων κώδικας). Lombok `@Slf4j` όπου ήδη χρησιμοποιείται.
- Μικρές, στοχευμένες αλλαγές· μην κάνεις reformat αρχείων (το `application.yaml` έχει ήδη ανεπιθύμητες αλλαγές indentation στο git).
- Άλλαξες το `.proto` ή το mapping; Ενημέρωσε τα tests στο `src/test/.../mapping/` (`RecordMapperTest`, `ProtobufRegistryRoundTripTest` με mock registry, χωρίς Kafka).
