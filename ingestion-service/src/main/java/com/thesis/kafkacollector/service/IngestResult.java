package com.thesis.kafkacollector.service;

/**
 * Το αποτέλεσμα μιας εισαγωγής. Το {@code accepted} είναι όσα <b>επιβεβαίωσε</b> το Kafka (μετά
 * από flush), όχι όσα μπήκαν στην ουρά του producer — η διαφορά φαίνεται μόνο όταν πέσει ο broker,
 * και είναι η διαφορά ανάμεσα στο «έφυγαν» και στο «χάθηκαν».
 *
 * @param read     πόσες εγγραφές διαβάστηκαν από την είσοδο
 * @param accepted πόσες επιβεβαιώθηκαν από το Kafka
 * @param skipped  πόσες ήταν άκυρες και προσπεράστηκαν
 */
public record IngestResult(int read, int accepted, int skipped) {
    public int failed() {
        return read - skipped - accepted;
    }
}
