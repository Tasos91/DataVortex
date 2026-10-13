-- Τα lat/lon ήταν πεδία-παράδειγμα και φεύγουν: ο πίνακας δεν ξέρει τίποτα για το νόημα των
-- δεδομένων. Ό,τι ορίζει ο χρήστης στα fieldMappings ζει στο jsonb "fields".
ALTER TABLE ingested_records DROP COLUMN IF EXISTS lat;
ALTER TABLE ingested_records DROP COLUMN IF EXISTS lon;
