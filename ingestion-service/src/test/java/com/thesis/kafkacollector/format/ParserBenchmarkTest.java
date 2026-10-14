package com.thesis.kafkacollector.format;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.kafkacollector.csv.CsvReader;
import com.thesis.kafkacollector.proto.IngestedRecordProto.IngestedRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Πόσο γρήγορα διαβάζεται κάθε μορφή, και — το ζητούμενο — <b>πόσο την επιβαρύνει η δυσκολία της
 * δομής</b>.
 * <p>
 * Δεν αρκεί να είναι γρήγορο το εύκολο. Το ερώτημα είναι αν μια δομή με δέκα επίπεδα φωλιάς ή με
 * εκατό πεδία ανά εγγραφή καταρρέει την ταχύτητα. Κάθε μέτρηση περιλαμβάνει ολόκληρη τη διαδρομή
 * μέχρι το έτοιμο μήνυμα Protobuf — ανάγνωση, επιλογή εγγραφών, ανάγνωση πεδίων, μετατροπή τύπων.
 * <p>
 * Τα νούμερα τυπώνονται· κανένα κατώφλι δεν ελέγχεται, γιατί ένα κατώφλι σε κοινόχρηστο μηχάνημα
 * θα έσπαγε τυχαία. Αυτό που ελέγχεται είναι η <b>ορθότητα</b>: ότι κάθε διαδρομή παρήγαγε όσες
 * εγγραφές και όσα πεδία έπρεπε.
 */
class ParserBenchmarkTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int N = 50_000;
    private static final List<String> REPORT = new ArrayList<>();

    /** Τρέχει τη μέτρηση δύο φορές και κρατά την καλύτερη: η πρώτη πληρώνει το ζέσταμα του JIT. */
    private static void measure(String label, int records, int fieldsPerRecord, Work work) throws Exception {
        measure(label, records, fieldsPerRecord, 0, work);
    }

    /**
     * @param inputBytes το μέγεθος της εισόδου. Είναι η <b>κρίσιμη</b> στήλη: μια δομή με δέκα
     *                   επίπεδα είναι διπλάσια σε bytes από την επίπεδη με τα ίδια πεδία, άρα
     *                   λιγότερες εγγραφές/s δεν σημαίνει απαραίτητα ότι το βάθος κοστίζει.
     */
    private static void measure(String label, int records, int fieldsPerRecord, int inputBytes, Work work)
            throws Exception {
        work.run(); // ζέσταμα
        long best = Long.MAX_VALUE;
        for (int i = 0; i < 2; i++) {
            long started = System.nanoTime();
            long produced = work.run();
            best = Math.min(best, System.nanoTime() - started);
            assertEquals(records, produced, label + ": λάθος πλήθος εγγραφών");
        }
        long ms = Math.max(1, best / 1_000_000);
        String throughput = inputBytes > 0
                ? String.format("%6.1f MB/s", inputBytes / 1048576.0 * 1000 / ms)
                : "           ";
        REPORT.add(String.format("%-30s %4d πεδία  %5d ms  %,10d εγγρ/s  %,11d πεδία/s  %s",
                label, fieldsPerRecord, ms,
                records * 1000L / ms, (long) records * fieldsPerRecord * 1000L / ms, throughput));
    }

    @FunctionalInterface
    private interface Work {
        long run() throws Exception;
    }

    // ------------------------------------------------------------- γεννήτριες

    private static String flatCsv() {
        StringBuilder sb = new StringBuilder(N * 70).append("ts,host,cpu,lat,mem,err\n");
        for (int i = 0; i < N; i++) {
            sb.append(1790000000000L + i * 10L).append(",node-").append(i % 12)
              .append(",87.5,42.1,13.25,2\n");
        }
        return sb.toString();
    }

    private static String flatNdjson() {
        StringBuilder sb = new StringBuilder(N * 110);
        for (int i = 0; i < N; i++) {
            sb.append("{\"ts\":").append(1790000000000L + i * 10L)
              .append(",\"host\":\"node-").append(i % 12)
              .append("\",\"cpu\":87.5,\"lat\":42.1,\"mem\":13.25,\"err\":2}\n");
        }
        return sb.toString();
    }

    /** Ίδια πεδία, αλλά θαμμένα τρία επίπεδα βαθιά. */
    private static String nestedNdjson() {
        StringBuilder sb = new StringBuilder(N * 160);
        for (int i = 0; i < N; i++) {
            sb.append("{\"meta\":{\"ts\":").append(1790000000000L + i * 10L)
              .append(",\"src\":{\"host\":\"node-").append(i % 12)
              .append("\"}},\"metrics\":{\"sys\":{\"cpu\":87.5,\"mem\":13.25},")
              .append("\"app\":{\"lat\":42.1,\"err\":2}}}\n");
        }
        return sb.toString();
    }

    /** Δέκα επίπεδα φωλιάς πριν φτάσεις στην τιμή. */
    private static String deepNdjson() {
        StringBuilder sb = new StringBuilder(N * 220);
        for (int i = 0; i < N; i++) {
            sb.append("{\"ts\":").append(1790000000000L + i * 10L).append(',');
            sb.append("\"d\":");
            for (int d = 0; d < 10; d++) {
                sb.append("{\"l").append(d).append("\":");
            }
            sb.append("{\"cpu\":87.5,\"lat\":42.1,\"mem\":13.25,\"err\":2}");
            sb.append("}".repeat(10)).append("}\n");
        }
        return sb.toString();
    }

    private static String arrayJson() {
        StringBuilder sb = new StringBuilder(N * 110).append("{\"data\":{\"rows\":[");
        for (int i = 0; i < N; i++) {
            if (i > 0) sb.append(',');
            sb.append("{\"ts\":").append(1790000000000L + i * 10L)
              .append(",\"host\":\"node-").append(i % 12)
              .append("\",\"cpu\":87.5,\"lat\":42.1,\"mem\":13.25,\"err\":2}");
        }
        return sb.append("]}}").toString();
    }

    /** Οντότητες με λίστες μετρήσεων: ξεδίπλωμα δύο επιπέδων, πεδία και από τον γονέα. */
    private static String explodingJson() {
        int groups = 50;
        int perGroup = N / groups;
        StringBuilder sb = new StringBuilder(N * 90).append("{\"stations\":[");
        for (int g = 0; g < groups; g++) {
            if (g > 0) sb.append(',');
            sb.append("{\"id\":\"st-").append(g).append("\",\"readings\":[");
            for (int i = 0; i < perGroup; i++) {
                if (i > 0) sb.append(',');
                sb.append("{\"ts\":").append(1790000000000L + i * 10L)
                  .append(",\"cpu\":87.5,\"lat\":42.1,\"mem\":13.25,\"err\":2}");
            }
            sb.append("]}");
        }
        return sb.append("]}").toString();
    }

    private static String xml(boolean attributes) {
        StringBuilder sb = new StringBuilder(N * 150).append("<rs>");
        for (int i = 0; i < N; i++) {
            if (attributes) {
                sb.append("<r ts=\"").append(1790000000000L + i * 10L).append("\" host=\"node-")
                  .append(i % 12).append("\"><cpu>87.5</cpu><lat>42.1</lat><mem>13.25</mem><err>2</err></r>");
            } else {
                sb.append("<r><ts>").append(1790000000000L + i * 10L).append("</ts><host>node-")
                  .append(i % 12).append("</host><cpu>87.5</cpu><lat>42.1</lat><mem>13.25</mem><err>2</err></r>");
            }
        }
        return sb.append("</rs>").toString();
    }

    /** Εκατό πεδία ανά εγγραφή, από τα οποία κρατάμε τα 20. */
    private static String wideNdjson(int rows) {
        StringBuilder sb = new StringBuilder(rows * 1200);
        for (int i = 0; i < rows; i++) {
            sb.append('{');
            for (int f = 0; f < 100; f++) {
                if (f > 0) sb.append(',');
                sb.append("\"f").append(f).append("\":").append(f).append(".5");
            }
            sb.append("}\n");
        }
        return sb.toString();
    }

    // ------------------------------------------------------------- βοηθητικά

    private static List<FieldSpec.Compiled> fields(String... pathsAndTypes) {
        List<FieldSpec.Compiled> out = new ArrayList<>();
        for (int i = 0; i < pathsAndTypes.length; i += 2) {
            out.add(FieldSpec.of("f" + i, pathsAndTypes[i], pathsAndTypes[i + 1]).compile());
        }
        return out;
    }

    /** Χτίζει το μήνυμα όπως ακριβώς η παραγωγή: μία διάσχιση για όλα τα πεδία. */
    private static int build(JsonNode record, RecordPath.Scope scope, List<FieldSpec.Compiled> fields) {
        IngestedRecord.Builder b = IngestedRecord.newBuilder().setSourceId("bench");
        int n = 0;
        for (FieldSpec.Compiled field : fields) {
            com.google.protobuf.Value v = field.read(record, scope);
            if (v != null) {
                b.putFields(field.name(), v);
                n++;
            }
        }
        b.build();
        return n;
    }

    private static long ndjson(String text, List<FieldSpec.Compiled> fields, AtomicLong fieldCount)
            throws Exception {
        long records = 0;
        fieldCount.set(0);
        var reader = MAPPER.reader();
        try (var br = new java.io.BufferedReader(new StringReader(text), 1 << 16)) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.isBlank()) continue;
                fieldCount.addAndGet(build(reader.readTree(line), null, fields));
                records++;
            }
        }
        return records;
    }

    // ------------------------------------------------------------- η μέτρηση

    @Test
    @DisplayName("ρυθμός ανά μορφή και ανά δυσκολία δομής")
    void benchmark() throws Exception {
        AtomicLong fieldCount = new AtomicLong();

        // --- CSV: θέσεις σε πίνακα, καμία δομή ---
        String csv = flatCsv();
        List<FieldSpec.Compiled> csvFields = fields("cpu", "number", "lat", "number", "mem", "number", "err", "number");
        measure("CSV επίπεδο", N, 4, csv.length(), () -> {
            long rows = 0;
            try (CsvReader r = new CsvReader(new StringReader(csv), ',')) {
                String[] row = r.next(); // επικεφαλίδες
                while ((row = r.next()) != null) {
                    if (CsvReader.isBlank(row)) continue;
                    IngestedRecord.Builder b = IngestedRecord.newBuilder().setSourceId("bench");
                    for (int i = 2; i <= 5; i++) {
                        b.putFields("f" + i, com.thesis.kafkacollector.mapping.Values
                                .ofNumber(Double.parseDouble(row[i])));
                    }
                    b.build();
                    rows++;
                }
            }
            return rows;
        });

        // --- NDJSON σε τρία επίπεδα δυσκολίας ---
        String flat = flatNdjson();
        List<FieldSpec.Compiled> flatFields =
                fields("$.cpu", "number", "$.lat", "number", "$.mem", "number", "$.err", "number");
        measure("NDJSON επίπεδο", N, 4, flat.length(), () -> ndjson(flat, flatFields, fieldCount));
        assertEquals(4L * N, fieldCount.get());

        String nested = nestedNdjson();
        List<FieldSpec.Compiled> nestedFields = fields(
                "$.metrics.sys.cpu", "number", "$.metrics.app.lat", "number",
                "$.metrics.sys.mem", "number", "$.metrics.app.err", "number");
        measure("NDJSON φωλιά 3 επιπέδων", N, 4, nested.length(), () -> ndjson(nested, nestedFields, fieldCount));
        assertEquals(4L * N, fieldCount.get());

        String deep = deepNdjson();
        String prefix = "$.d" + ".l0.l1.l2.l3.l4.l5.l6.l7.l8.l9".replace(".l", ".l");
        List<FieldSpec.Compiled> deepFields = fields(
                prefix + ".cpu", "number", prefix + ".lat", "number",
                prefix + ".mem", "number", prefix + ".err", "number");
        measure("NDJSON φωλιά 10 επιπέδων", N, 4, deep.length(), () -> ndjson(deep, deepFields, fieldCount));
        assertEquals(4L * N, fieldCount.get(), "σε δέκα επίπεδα βάθους πρέπει να βρεθούν ΟΛΑ");

        // --- εγγραφές με 100 πεδία, κρατάμε 20 ---
        int wideRows = N / 5;
        String wide = wideNdjson(wideRows);
        List<String> wideSpec = new ArrayList<>();
        for (int f = 0; f < 20; f++) {
            wideSpec.add("$.f" + f);
            wideSpec.add("number");
        }
        List<FieldSpec.Compiled> wideFields = fields(wideSpec.toArray(new String[0]));
        measure("NDJSON 100 πεδία (κρατάμε 20)", wideRows, 20, wide.length(),
                () -> ndjson(wide, wideFields, fieldCount));
        assertEquals(20L * wideRows, fieldCount.get());

        // --- JSON σε ροή, και με ξεδίπλωμα ---
        String array = arrayJson();
        measure("JSON array σε ροή", N, 4, array.length(), () -> {
            AtomicLong rows = new AtomicLong();
            JsonRecords.forEach(new StringReader(array), "$.data.rows[*]", MAPPER, record -> {
                build(record, null, flatFields);
                rows.incrementAndGet();
            });
            return rows.get();
        });

        String exploding = explodingJson();
        List<FieldSpec.Compiled> withParent = new ArrayList<>(flatFields);
        withParent.add(FieldSpec.of("station", "$^.id", "text").compile());
        measure("JSON ξεδίπλωμα + πεδίο γονέα", N, 5, exploding.length(), () -> {
            JsonNode root = MAPPER.readTree(exploding);
            AtomicLong rows = new AtomicLong();
            RecordPath.of("$.stations[*].readings[*]").forEachWithScope(root, (record, scope) -> {
                assertEquals(5, build(record, scope, withParent));
                rows.incrementAndGet();
            });
            return rows.get();
        });

        // --- XML ---
        String xmlFlat = xml(false);
        List<FieldSpec.Compiled> xmlFields =
                fields("$.cpu", "number", "$.lat", "number", "$.mem", "number", "$.err", "number");
        measure("XML στοιχεία", N, 4, xmlFlat.length(), () -> {
            AtomicLong rows = new AtomicLong();
            XmlRecords.forEach(new StringReader(xmlFlat), "r", record -> {
                build(record, null, xmlFields);
                rows.incrementAndGet();
            });
            return rows.get();
        });

        String xmlAttrs = xml(true);
        measure("XML με χαρακτηριστικά", N, 4, xmlAttrs.length(), () -> {
            AtomicLong rows = new AtomicLong();
            XmlRecords.forEach(new StringReader(xmlAttrs), "r", record -> {
                build(record, null, xmlFields);
                rows.incrementAndGet();
            });
            return rows.get();
        });

        System.out.println("\n╔══ Ρυθμός ανά μορφή και δυσκολία δομής ══════════════════════════════════════════════╗");
        REPORT.forEach(line -> System.out.println("  " + line));
        System.out.println("╚════════════════════════════════════════════════════════════════════════════════════╝\n");
    }
}
