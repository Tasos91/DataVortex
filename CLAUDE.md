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
- `controller/IngestionController`: `POST /api/v1/upload` (το μόνο write endpoint).
- `controller/DashboardController`: `/dashboard` (HTML) και `/dashboard/api/stats`.
- `service/FileStreamingService`: CSV, JSON αρχείο και inline `payload` → Protobuf → Kafka.
- `service/KafkaTopicService`: ελέγχει/δημιουργεί το topic του request (fail-fast, με cache).
- `mapping/JsonPathResolver` και `mapping/RecordMapper`: εφαρμόζουν τα `fieldMappings` στο payload.
- `config/KafkaConsumerConfig`: bean `Consumer<Message<List<IngestedRecord>>> consumeData` — **batch** consumer
  (Spring Cloud Stream). Τα Kafka metadata (topic/partition/offset) έρχονται ως Lists στα headers.
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
  Σε batch listener, σκέτο exception στέλνει ΟΛΟ το batch στο DLQ· πέτα `BatchListenerFailedException(msg, index)`
  για να πάει μόνο το ένα record.
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

## Στυλ
- Σχόλια και μηνύματα log στα ελληνικά (όπως ο υπάρχων κώδικας). Lombok `@Slf4j` όπου ήδη χρησιμοποιείται.
- Μικρές, στοχευμένες αλλαγές· μην κάνεις reformat αρχείων (το `application.yaml` έχει ήδη ανεπιθύμητες αλλαγές indentation στο git).
- Άλλαξες το `.proto` ή το mapping; Ενημέρωσε τα tests στο `src/test/.../mapping/` (`RecordMapperTest`, `ProtobufRegistryRoundTripTest` με mock registry, χωρίς Kafka).
