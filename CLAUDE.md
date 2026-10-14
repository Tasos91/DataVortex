# datavortex

Spring Boot 4 / Java 21, Maven multi-module. Δέχεται **οποιοδήποτε** JSON/CSV, ο χρήστης λέει ποια
πεδία θέλει (`fieldMappings`), τα στέλνει στο Kafka ως Protobuf (Schema Registry), ένας batch
consumer τα γράφει σε TimescaleDB, ένα read-only API τα σερβίρει και ένα UI τα αναλύει.

| Module | Τι είναι | Port |
|---|---|---|
| `common` | Protobuf σχήμα + Flyway migrations (μία πηγή αλήθειας) | — |
| `ingestion-service` (`com.thesis.kafkacollector`) | REST εισαγωγή → Kafka, consumer → βάση | 8080 |
| `api-service` (`...datavortex.api`) | read-only REST· **το μόνο** που διαβάζει τη βάση | 8082 |
| `analytics-ui-service` (`...datavortex.analytics`) | UI + clustering/classification· καλεί μόνο το api | 8085 |

Το UI δεν έχει driver βάσης στο classpath — η απομόνωση είναι δομική, όχι σύμβαση.

## Εντολές (αργή CPU, 4 πυρήνες: τρέχε το ελάχιστο)
`.claude/settings.json` ορίζει ήδη το `JAVA_HOME` (η default `java` είναι 11) — **μη βάζεις `export`**.

```bash
bash scripts/t.sh ingestion-service              # tests ενός module, σύνοψη 1 γραμμής
bash scripts/t.sh api-service BucketsTest        # ένα test
bash scripts/t.sh all                            # όλα (αργό, θέλει Docker για τα IT)
bash scripts/c.sh analytics-ui-service           # compile μόνο
bash scripts/proto.sh                            # ΜΕΤΑ από αλλαγή .proto ή migration
bash run-all.sh [ingestion|api|ui]               # τρέξιμο (logs στο logs/)
```
- Δούλεψε πάντα με `-pl <module>`, ποτέ σε όλο το reactor. Μην τρέχεις `package`/`clean` αδικαιολόγητα.
- ΜΗΝ τρέχεις `KafkaCollectorApplicationTests` (`@SpringBootTest`, θέλει βάση + Kafka).
- Το `IngestedRecordProto` **παράγεται** στο compile του `common`· μην το ψάχνεις στο `src/`.
- **Χωρίς Lombok** (αφαιρέθηκε): ρητό `private static final Logger log = LoggerFactory.getLogger(X.class);`.

## Στυλ
- Σχόλια, log, commit messages: **ελληνικά**. Το σχόλιο λέει *γιατί*, όχι τι.
- Μικρές στοχευμένες αλλαγές· κανένα reformat (το `application.yaml` έχει ήδη θορυβώδες indentation στο git).
- **Μία δουλειά = ένα branch**: branch → commit → push → `merge --no-ff` στο master → push.
- Κάθε αλλαγή: tests που τρέχουν + ενημέρωση αυτού του αρχείου.

## Κανόνες που ΔΕΝ αλλάζουν (ο καθένας έχει πληρωθεί με ένα bug)
**Kafka / εισαγωγή**
1. Μήνυμα = **Protobuf μέσω Schema Registry** (`:8081`, απαραίτητο και για producer και για consumer).
   Κανένα χειροκίνητο `parseFrom`. Δεν υπάρχει Avro. Ασύμβατη αλλαγή `.proto` → ο registry απορρίπτει.
2. Νέο πεδίο στο proto = **νέος αριθμός**. Τα 3,4 είναι `reserved` (ήταν lat/lon).
3. Τα δυναμικά πεδία ζουν **όλα** στο `map<string, Value> fields = 5`. Κανένα όνομα δεν έχει ειδική
   μεταχείριση. `fieldMappings`: `{"Όνομα": "$.path.στο.json"}`· σκέτο όνομα = κλειδί 1ου επιπέδου (CSV).
   Πεδίο που δεν βρέθηκε → παράλειψη + debug, όχι σφάλμα.
4. **Ο consumer ακούει σε pattern** `ingest\..*` (`destination-is-pattern: true`). Άρα τα topics πρέπει να
   λέγονται `ingest.<κάτι>`. Το `dlq.ingest` είναι επίτηδες **έξω** από το pattern (αλλιώς βρόχος).
5. **`keyField`** → partition: ίδιο κλειδί = εγγυημένη σειρά. ΜΗΝ βάλεις `sourceId` ως κλειδί (όλα σε ένα
   partition, μηδενίζει τον παραλληλισμό). Χωρίς `keyField` → μέγιστο throughput, καμία σειρά.
6. **`timeField`** → ο χρόνος του γεγονότος γίνεται timestamp του μηνύματος Kafka, άρα και `event_time`
   στη βάση. Χωρίς αυτό όλα τα ιστορικά δεδομένα εμφανίζονται «τώρα».
7. **Ο τύπος του consumer είναι `List<Object>`**, όχι `List<IngestedRecord>`: ένα μήνυμα που δεν
   διαβάστηκε έρχεται ως `KafkaNull` και με δηλωμένο τύπο σκάει `ClassCastException` που στέλνει
   **όλο** το batch (500 καλά μηνύματα) στο DLQ. Αντ' αυτού: `BatchListenerFailedException(msg, index)`.
8. **Πέφτει η βάση → τίποτα στο DLQ**: `setBackOffFunction` δίνει άπειρα retries στα σφάλματα υποδομής.
   Τα offsets δεν γίνονται commit, το Kafka κρατά τα δεδομένα. Δοκιμασμένο με τη βάση σβηστή 25s.
9. **ΜΗΝ αφαιρέσεις το `kafkaTemplate.flush()`** στο `finish()`: χωρίς αυτό το 200 σημαίνει «μπήκε στο
   buffer», όχι «είναι στο Kafka».
10. **Ο μικρός buffer είναι επίτηδες** (16MB, όχι 64MB) και το `max.block.ms: 60000` επίσης: το
    μπλοκάρισμα στο `send()` **είναι** το backpressure. Με 64MB το `delivery.timeout.ms` ΑΚΥΡΩΝΕ εγγραφές.
11. Το DLQ `KafkaTemplate` **δεν** είναι bean επίτηδες: δεύτερο `KafkaTemplate`/`ProducerFactory` στο
    context ακυρώνει το `KafkaAutoConfiguration`.
12. Τα topics δεν δημιουργούνται αυτόματα (`auto.create.topics.enable=false`, αλλιώς 1 partition).
    Είτε `createTopicIfMissing: true`, είτε χειροκίνητα.

**Βάση (TimescaleDB `2.30.2-pg17`, volume `tsdata`)**
13. **Χωρίς JPA/Hibernate**, μόνο `JdbcTemplate`: το Hibernate απενεργοποιεί το JDBC batching με
    `@GeneratedValue(IDENTITY)` → ένα INSERT ανά record.
14. **Το schema το φτιάχνει το Flyway** (`common/src/main/resources/db/migration/`), `ddl-auto: none`.
    Νέα αλλαγή = **νέο** `V5__...sql`· ΜΗΝ πειράξεις migration που έχει τρέξει. Στο Boot 4 χρειάζεται
    `spring-boot-starter-flyway` (μόνο με `flyway-core` δεν τρέχει τίποτα).
15. **Idempotency**: `event_time` = timestamp του μηνύματος Kafka (σταθερό σε replay), ποτέ `now()`.
    `PRIMARY KEY (event_time, topic, kafka_partition, kafka_offset)` + `ON CONFLICT DO NOTHING`.
    Δοκιμασμένο: πλήρες replay → 0 νέες γραμμές.
16. **Ο πίνακας δεν αποκτά στήλες ανά mapping**: νέο πεδίο = νέο κλειδί στο `fields` jsonb.
17. **Ένα statement γράφει σε τρεις πίνακες** (`ingested_records` → `metric_points` → `field_catalog`) με
    data-modifying CTEs και κοινό `jsonb_each`. Το `ORDER BY 1,2,3,4` στον κατάλογο **δεν** είναι
    διακόσμηση: χωρίς αυτό παράλληλοι consumers κάνουν deadlock (φάνηκε στις 100.000 εγγραφές).
18. Το TimescaleDB απαιτεί τη διάσταση χρόνου σε κάθε unique index και τα `segmentby` μέσα σ' αυτό —
    γι' αυτό `segmentby = topic` μόνο. **Καμία retention policy** (θα έσβηνε δεδομένα).
19. Το read-only του api-service επιβάλλεται **από τον server**
    (`options=-c default_transaction_read_only=on`): το `readOnly` του Hikari/pgjdbc ισχύει μόνο μέσα σε
    ρητό transaction και με autocommit τα INSERT περνούσαν.

**Ανάλυση / API**
20. **Ο χρόνος στρογγυλοποιείται στα 30s** (`TimeRange.GRAIN`, ίδιο και στο api-service): με «μέχρι
    τώρα» το κλειδί του cache άλλαζε κάθε ms και το cache **δεν χτυπούσε ποτέ**.
21. **Διάμεσος/MAD ποτέ μέσα σε βρόχο σημείων** (ήταν O(n² log n): 252ms → 8,8ms).
22. **Κατώφλια → μέσοι όροι, ανωμαλίες → αιχμές.** Σε κουβά με 800 μετρήσεις η max ξεπερνά κάθε
    κατώφλι («μια στάλα βροχή» → βροχή στο 100% του χρόνου). Το `/matrix` δέχεται `agg=avg|max|min`.
23. **Η ομαδοποίηση αφορά χρονικά παράθυρα, όχι εγγραφές** — γι' αυτό δεν αργεί ποτέ: 10 εκατ. εγγραφές
    σε 24h δίνουν τους ίδιους ~600 κουβάδες με 10 χιλιάδες.
24. **Κενά ≠ μηδέν**: `NaN` + `Stats.present()`, imputation με διάμεσο.
25. Νέος τύπος δεδομένων = **μία κλάση** που υλοποιεί το `DataProfile` και δηλώνεται bean. Το
    `ProfileRegistry` διαλέγει με βαθμολογία, όχι με πίνακα· το `generic` βαθμολογεί πάντα 1.

**Spring Boot 4 / Jackson**
26. Το MVC χρησιμοποιεί **Jackson 3** (`tools.jackson`), ο κώδικας **Jackson 2** (`com.fasterxml`).
    Γι' αυτό το `payload` του DTO είναι `Object`, όχι `JsonNode` — μην το αλλάξεις.
27. `MeterRegistryCustomizer` → `org.springframework.boot.micrometer.metrics.autoconfigure`.
    `RestClient.Builder` **δεν** είναι auto-configured.
28. Testcontainers 2.x: `testcontainers-postgresql`/`testcontainers-junit-jupiter`, BOM ρητά, χρειάζεται
    `commons-io`, εικόνα με `asCompatibleSubstituteFor("postgres")`, προσοχή στο διπλό `?` του `getJdbcUrl()`.
29. Το ETag εμπόδιζε τη συμπίεση (205KB → 28KB μόλις αφαιρέθηκε). Ο Tomcat σερβίρει `.js` ως
    `text/javascript` — βάλ' το στα `compression.mime-types`.

## Endpoints
**ingestion (`:8080`)** — `200` μετά τα acks, `207` μερική αποτυχία, `400` λάθος request,
`413` πάνω από `max-upload-bytes`, `429` πάνω από `max-concurrent-ingests`, `503` νεκρός broker.
| Endpoint | Τι κάνει |
|---|---|
| `POST /api/v1/upload` | JSON με `payload` μέσα, ή `fileType`+`filePath`. Όλο στη μνήμη. |
| `POST /api/v1/stream` | **NDJSON**: 1η γραμμή ρυθμίσεις, μετά 1 εγγραφή/γραμμή. Μία γραμμή στη μνήμη. |
| `POST /api/v1/csv/inspect` | CSV (multipart ή raw) → στήλες, τύποι, δείγματα, **έτοιμες προτάσεις** mapping |
| `POST /api/v1/csv/ingest` | multipart `file` + `plan` → streaming εισαγωγή με τα mappings του χρήστη |
| `GET /csv` | η σελίδα επιλογής στηλών (drag & drop) |
| `GET /dashboard` | στατιστικά εισαγωγής |

**api (`:8082`)** — `sources`, `sources/{s}/fields`, `series`, `matrix` (κουβάδες × πεδία, για ML),
`rates`, `records` (keyset, max 1000). Όλα κάτω από `/api/v1`.

**ui (`:8085`)** — `static/index.html` (vanilla JS + τοπικό ECharts), μιλά μόνο στο δικό του `/api/**`.

## Υποδομή
`docker compose up -d postgres kafka schema-registry prometheus grafana`. Η εφαρμογή δεν ξεκινά χωρίς
βάση (Flyway στο startup). Grafana `:3000` admin/admin, δύο provisioned dashboards. Prometheus `:9090`
(σε Linux χρειάζεται `extra_hosts: host.docker.internal:host-gateway`).
Υγεία με νόημα: ingestion → `kafka`+`db`, api → `db`, ui → `apiService` (+latency).
Μετρικές: `datavortex.{ingest.records,ingest.flush,consume.records,consume.batch.size,db.insert,db.rows}`
και `datavortex.kafka.consumer.lag` (AdminClient/15s, **μόνο** για topics του pattern — αλλιώς ψεύτικο lag).

## Πού είναι τα υπόλοιπα
- **[docs/NOTES.md](docs/NOTES.md)** — το πλήρες ιστορικό: μετρημένες αποδόσεις, τι βρήκε κάθε πέρασμα
  απόδοσης/UI, απόδειξη κλίμακας (7 εκατ. σημεία), τι έπιασαν τα integration tests. Διάβασέ το **μόνο**
  όταν ψάχνεις συγκεκριμένο νούμερο ή την αιτία μιας απόφασης.
- **[README.md](README.md)** — η ματιά του χρήστη.
- Tests: δες `scripts/t.sh`. Τελευταία μέτρηση στο NOTES.
