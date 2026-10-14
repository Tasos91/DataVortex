package com.thesis.kafkacollector.mapping;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;

import java.util.Iterator;
import java.util.Map;

/**
 * Μετατροπή μιας τιμής JSON σε τιμή Protobuf, με τις επαναλαμβανόμενες τιμές έτοιμες.
 * <p>
 * Κάθε {@code Value.newBuilder()...build()} δεσμεύει δύο αντικείμενα. Σε μια εισαγωγή 200.000
 * εγγραφών με έξι πεδία η καθεμία, αυτό είναι 2,4 εκατομμύρια αντικείμενα — για τιμές που πολύ
 * συχνά είναι <b>οι ίδιες</b>: το {@code true}, το {@code null}, το μηδέν, ένας μετρητής σφαλμάτων
 * που είναι σχεδόν πάντα 0 ή 1.
 * <p>
 * Οι τιμές του Protobuf είναι <b>αμετάβλητες</b>, άρα μπορούν να μοιράζονται με απόλυτη ασφάλεια
 * ανάμεσα σε νήματα και μηνύματα. Εδώ κρατούνται έτοιμες όσες είναι σίγουρα επαναλαμβανόμενες και
 * φτηνές να αναγνωριστούν: τα δύο boolean, το κενό, και οι μικροί ακέραιοι.
 * <p>
 * Για κείμενο <b>δεν</b> κρατάμε μνήμη επίτηδες: τα διαφορετικά αλφαριθμητικά μιας πηγής μπορεί να
 * είναι εκατομμύρια (π.χ. αναγνωριστικά), και μια μνήμη που μεγαλώνει χωρίς όριο είναι χειρότερο
 * πρόβλημα από αυτό που λύνει.
 */
public final class Values {

    private static final Value TRUE = Value.newBuilder().setBoolValue(true).build();
    private static final Value FALSE = Value.newBuilder().setBoolValue(false).build();
    private static final Value NULL = Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();

    /** Οι ακέραιοι 0..255 καλύπτουν μετρητές, σημαίες, κωδικούς και ποσοστά — ό,τι επαναλαμβάνεται. */
    private static final int SMALL_MAX = 255;
    private static final Value[] SMALL = new Value[SMALL_MAX + 1];

    static {
        for (int i = 0; i <= SMALL_MAX; i++) {
            SMALL[i] = Value.newBuilder().setNumberValue(i).build();
        }
    }

    private Values() {
    }

    public static Value ofBoolean(boolean b) {
        return b ? TRUE : FALSE;
    }

    public static Value ofNull() {
        return NULL;
    }

    /** Αριθμός, με τους μικρούς ακέραιους έτοιμους. */
    public static Value ofNumber(double d) {
        int i = (int) d;
        if (i == d && i >= 0 && i <= SMALL_MAX) {
            return SMALL[i];
        }
        return Value.newBuilder().setNumberValue(d).build();
    }

    public static Value ofString(String s) {
        return Value.newBuilder().setStringValue(s).build();
    }

    /** Ο τύπος του κόμβου καθορίζει τον τύπο της τιμής — καμία μετατροπή, καμία εικασία. */
    public static Value of(JsonNode node) {
        if (node.isNumber()) {
            return ofNumber(node.asDouble());
        } else if (node.isTextual()) {
            return ofString(node.asText());
        } else if (node.isBoolean()) {
            return ofBoolean(node.asBoolean());
        } else if (node.isNull() || node.isMissingNode()) {
            return NULL;
        } else if (node.isArray()) {
            ListValue.Builder list = ListValue.newBuilder();
            for (JsonNode child : node) {
                list.addValues(of(child));
            }
            return Value.newBuilder().setListValue(list).build();
        } else if (node.isObject()) {
            Struct.Builder struct = Struct.newBuilder();
            Iterator<Map.Entry<String, JsonNode>> it = node.fields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> e = it.next();
                struct.putFields(e.getKey(), of(e.getValue()));
            }
            return Value.newBuilder().setStructValue(struct).build();
        }
        return ofString(node.asText());
    }
}
