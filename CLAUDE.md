# datavortex

Spring Boot 4 / Java 21 εφαρμογή (διπλωματική): δέχεται δεδομένα μέσω REST, τα μετατρέπει σε
Protobuf και τα στέλνει στο Kafka· ένας consumer τα διαβάζει πίσω και τροφοδοτεί ένα dashboard.
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
- `service/FileStreamingService`: CSV, JSON αρχείο και inline `payload` → Protobuf → Kafka (`KafkaTemplate<String, byte[]>`).
- `mapping/JsonPathResolver` και `mapping/RecordMapper`: εφαρμόζουν τα `fieldMappings` στο payload.
- `config/KafkaConsumerConfig`: bean `Consumer<byte[]> consumeData` (Spring Cloud Stream) → `DashboardMetricsService`.
- `service/KafkaConsumerService`: ολόκληρο σε σχόλια, νεκρός κώδικας· μην τον ξαναενεργοποιείς χωρίς λόγο.
- `entity/IngestedRecord` + `repository/`: JPA για Postgres (πίνακας `ingested_records`). Προς το παρόν ο consumer ΔΕΝ γράφει εκεί.
- `dto/IngestionRequest`: `sourceId`, `fileType`, `filePath`, `payload`, `fieldMappings`.

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
- Στο Spring Boot 4 το MVC χρησιμοποιεί Jackson 3, ενώ ο κώδικας χρησιμοποιεί `com.fasterxml` (Jackson 2). Γι' αυτό το
  `payload` στο DTO είναι `Object` και όχι `JsonNode`· μην το αλλάξεις.

## Ρυθμίσεις και υποδομή
- `application.yaml`: port 8080, topic `ais-data` (`KAFKA_TOPIC`), brokers `localhost:9092`, Postgres `localhost:5432/bigdata_db`.
  Όλα με env overrides· `ddl-auto: update`.
- `docker-compose.yml` (στη ρίζα): postgres:15, kafka (cp 7.6.0, KRaft), schema-registry (:8081, ΑΠΑΡΑΙΤΗΤΟ για producer και consumer), prometheus, grafana.
  Δεν έχει όρια μνήμης. Ξεκίνα μόνο ό,τι χρειάζεται: `docker compose up -d postgres kafka schema-registry`.
- Το topic δεν δημιουργείται αυτόματα (`auto-create-topics: false`)· πρέπει να υπάρχει πριν την αποστολή.

## Στυλ
- Σχόλια και μηνύματα log στα ελληνικά (όπως ο υπάρχων κώδικας). Lombok `@Slf4j` όπου ήδη χρησιμοποιείται.
- Μικρές, στοχευμένες αλλαγές· μην κάνεις reformat αρχείων (το `application.yaml` έχει ήδη ανεπιθύμητες αλλαγές indentation στο git).
- Άλλαξες το `.proto` ή το mapping; Ενημέρωσε τα tests στο `src/test/.../mapping/` (`RecordMapperTest`, `ProtobufRegistryRoundTripTest` με mock registry, χωρίς Kafka).
