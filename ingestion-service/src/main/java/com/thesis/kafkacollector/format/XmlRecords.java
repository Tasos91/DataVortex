package com.thesis.kafkacollector.format;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.Reader;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.function.Consumer;

/**
 * Διαβάζει XML σε ροή και παραδίδει κάθε εγγραφή ως δέντρο JSON, ώστε από εκεί και πέρα να
 * ακολουθεί <b>ακριβώς την ίδια</b> διαδρομή με το JSON: ίδιες διαδρομές πεδίων, ίδιοι
 * μετασχηματισμοί, ίδια επιθεώρηση.
 * <p>
 * Είναι streaming με StAX: στη μνήμη βρίσκεται <b>μία εγγραφή</b> τη φορά, όχι το έγγραφο. Ένα
 * αρχείο XML 5 GB περνά με τη μνήμη μιας εγγραφής. Δεν προστέθηκε καμία βιβλιοθήκη — το StAX
 * είναι μέρος του JDK.
 * <p>
 * Συμβάσεις μετατροπής, οι συνηθισμένες:
 * <ul>
 *   <li>στοιχείο με μόνο κείμενο → τιμή· το κείμενο μένει κείμενο, ο τύπος αποφασίζεται αργότερα
 *       από το {@link FieldSpec} (το XML δεν έχει τύπους, όπως και το CSV)·</li>
 *   <li>στοιχείο με παιδιά → αντικείμενο·</li>
 *   <li>επαναλαμβανόμενα αδέρφια με το ίδιο όνομα → πίνακας·</li>
 *   <li>χαρακτηριστικά (attributes) → πεδία με πρόθεμα {@code @}, π.χ. {@code $['@id']}.</li>
 * </ul>
 */
public final class XmlRecords {

    private static final JsonNodeFactory NODES = JsonNodeFactory.instance;
    /** Το κλειδί για το κείμενο ενός στοιχείου που έχει ΚΑΙ χαρακτηριστικά ΚΑΙ κείμενο. */
    public static final String TEXT_KEY = "#text";

    private XmlRecords() {
    }

    /**
     * @param recordElement το όνομα του στοιχείου που αποτελεί μία εγγραφή (π.χ. {@code reading}).
     *                      Κενό σημαίνει «τα παιδιά της ρίζας».
     * @param action        καλείται για κάθε εγγραφή που βρίσκεται
     * @return πόσες εγγραφές διαβάστηκαν
     */
    public static int forEach(Reader in, String recordElement, Consumer<JsonNode> action)
            throws XMLStreamException {
        return forEachWithScope(in, recordElement, (record, scope) -> action.accept(record));
    }

    /**
     * Το ίδιο, αλλά με πρόσβαση στα στοιχεία που <b>περιέχουν</b> κάθε εγγραφή.
     * <p>
     * Η τυπική δομή XML είναι {@code <station id="ATH"><reading/>…</station>}: η ταυτότητα της
     * οντότητας είναι χαρακτηριστικό του γονέα, όχι της εγγραφής. Χωρίς αυτό, κάθε μέτρηση θα
     * έχανε τον σταθμό της — όπως ακριβώς συνέβαινε και στο JSON πριν μπουν οι πρόγονοι.
     * <p>
     * Σε ροή ο γονέας δεν έχει διαβαστεί ολόκληρος όταν φτάνουμε στο παιδί. Κρατάμε ό,τι είναι ήδη
     * γνωστό: τα <b>χαρακτηριστικά</b> του (διαβάστηκαν με το άνοιγμά του) και όσα απλά παιδιά
     * προηγήθηκαν της εγγραφής. Στην πράξη η ταυτότητα βρίσκεται πάντα εκεί.
     */
    public static int forEachWithScope(Reader in, String recordElement,
                                       java.util.function.BiConsumer<JsonNode, RecordPath.Scope> action)
            throws XMLStreamException {
        XMLStreamReader reader = factory().createXMLStreamReader(in);
        String wanted = recordElement == null || recordElement.isBlank() ? null : recordElement.trim();
        Deque<ObjectNode> open = new ArrayDeque<>(8);
        int depth = 0;
        int count = 0;
        try {
            while (reader.hasNext()) {
                int event = reader.next();
                if (event == XMLStreamConstants.END_ELEMENT) {
                    if (!open.isEmpty()) {
                        open.pop();
                    }
                    depth--;
                    continue;
                }
                if (event != XMLStreamConstants.START_ELEMENT) {
                    continue;
                }
                depth++;
                boolean isRecord = wanted != null
                        ? wanted.equals(reader.getLocalName())
                        : depth == 2; // χωρίς όνομα: τα παιδιά της ρίζας είναι οι εγγραφές
                if (isRecord) {
                    JsonNode record = readElement(reader);
                    action.accept(record, new RecordPath.Scope(root(open), new ArrayDeque<>(open)));
                    count++;
                    depth--; // το readElement κατανάλωσε και το END_ELEMENT
                } else {
                    ObjectNode node = NODES.objectNode();
                    for (int i = 0; i < reader.getAttributeCount(); i++) {
                        node.put('@' + reader.getAttributeLocalName(i), reader.getAttributeValue(i));
                    }
                    open.push(node);
                }
            }
        } finally {
            reader.close();
        }
        return count;
    }

    /** Η «ρίζα» για τα πεδία με {@code $$}: το πιο εξωτερικό ανοιχτό στοιχείο. */
    private static JsonNode root(Deque<ObjectNode> open) {
        JsonNode outermost = null;
        for (ObjectNode node : open) {
            outermost = node;
        }
        return outermost == null ? NODES.objectNode() : outermost;
    }

    /**
     * Διαβάζει ολόκληρο το τρέχον στοιχείο (ο reader είναι σε START_ELEMENT) και το επιστρέφει ως
     * δέντρο. Σταματά αφού καταναλώσει το αντίστοιχο END_ELEMENT.
     */
    static JsonNode readElement(XMLStreamReader reader) throws XMLStreamException {
        ObjectNode node = NODES.objectNode();
        for (int i = 0; i < reader.getAttributeCount(); i++) {
            node.put('@' + reader.getAttributeLocalName(i), reader.getAttributeValue(i));
        }
        StringBuilder text = new StringBuilder();
        boolean hasChildren = false;

        while (reader.hasNext()) {
            int event = reader.next();
            switch (event) {
                case XMLStreamConstants.START_ELEMENT -> {
                    hasChildren = true;
                    String name = reader.getLocalName();
                    JsonNode child = readElement(reader);
                    add(node, name, child);
                }
                case XMLStreamConstants.CHARACTERS, XMLStreamConstants.CDATA -> text.append(reader.getText());
                case XMLStreamConstants.END_ELEMENT -> {
                    String value = text.toString().strip();
                    if (!hasChildren && node.isEmpty()) {
                        // Σκέτο κείμενο: η τιμή είναι το ίδιο το στοιχείο
                        return NODES.textNode(value);
                    }
                    if (!value.isEmpty()) {
                        node.put(TEXT_KEY, value);
                    }
                    return node;
                }
                default -> { /* σχόλια, οδηγίες επεξεργασίας: αγνοούνται */ }
            }
        }
        return node;
    }

    /** Δεύτερο αδέρφι με το ίδιο όνομα: το πεδίο γίνεται πίνακας. */
    private static void add(ObjectNode node, String name, JsonNode child) {
        JsonNode existing = node.get(name);
        if (existing == null) {
            node.set(name, child);
        } else if (existing.isArray()) {
            ((ArrayNode) existing).add(child);
        } else {
            ArrayNode array = NODES.arrayNode();
            array.add(existing);
            array.add(child);
            node.set(name, array);
        }
    }

    /**
     * Ο reader με τις εξωτερικές οντότητες <b>κλειστές</b>.
     * <p>
     * Χωρίς αυτό, ένα XML από άγνωστη πηγή μπορεί να ζητήσει αρχεία του δίσκου ή διευθύνσεις του
     * δικτύου μέσω DTD (η επίθεση «XXE»). Εδώ δεχόμαστε αρχεία που ανεβάζει ο χρήστης, άρα ο
     * parser πρέπει να είναι κλειστός εξαρχής και όχι «αν θυμηθούμε».
     */
    private static XMLInputFactory factory() {
        XMLInputFactory factory = XMLInputFactory.newInstance();
        factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
        factory.setProperty(XMLInputFactory.IS_COALESCING, true);
        return factory;
    }
}
