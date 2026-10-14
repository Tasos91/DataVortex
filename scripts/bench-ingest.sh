#!/usr/bin/env bash
# Μετρά ΟΛΟ τον σωλήνα: αρχείο → HTTP → Kafka → consumer → βάση.
#
# Δύο χρόνοι, γιατί είναι δύο διαφορετικά πράγματα:
#   HTTP   = διάβασμα, mapping, Protobuf, αποστολή και acks του Kafka
#   ΒΑΘΟΣ  = μέχρι να φτάσει η ΤΕΛΕΥΤΑΙΑ εγγραφή στη βάση (δηλαδή ο consumer να προλάβει)
#
#   scripts/bench-ingest.sh <csv> <plan.json> <sourceId> [αναμενόμενες_γραμμές]
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

CSV="${1:?δώσε csv}"
PLAN="${2:?δώσε plan.json}"
SOURCE="${3:?δώσε sourceId}"
EXPECTED="${4:-0}"
PSQL=(docker exec thesis-postgres psql -U postgres -d bigdata_db -tAc)

rows() { "${PSQL[@]}" "SELECT count(*) FROM ingested_records WHERE source_id='$SOURCE';" 2>/dev/null || echo 0; }
points() { "${PSQL[@]}" "SELECT count(*) FROM metric_points WHERE source_id='$SOURCE';" 2>/dev/null || echo 0; }

BEFORE=$(rows)
T0=$(date +%s%3N)
RESP=$(curl -s -X POST localhost:8080/api/v1/csv/ingest -F "file=@$CSV" -F "plan=@$PLAN;type=application/json")
T_HTTP=$(($(date +%s%3N) - T0))

ACCEPTED=$(printf '%s' "$RESP" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("accepted", 0))' 2>/dev/null || echo 0)
[ "$EXPECTED" = "0" ] && EXPECTED="$ACCEPTED"

# Περιμένουμε να σταματήσει να ανεβαίνει ο μετρητής, όχι απλώς να φτάσει τον στόχο:
# έτσι η μέτρηση ισχύει και όταν κάτι χάθηκε.
LAST=-1; STABLE=0; T_DB=0
for _ in $(seq 1 600); do
  N=$(rows)
  if [ "$N" -ge $((BEFORE + EXPECTED)) ]; then T_DB=$(($(date +%s%3N) - T0)); break; fi
  if [ "$N" = "$LAST" ]; then STABLE=$((STABLE + 1)); else STABLE=0; LAST=$N; fi
  [ "$STABLE" -ge 15 ] && { T_DB=$(($(date +%s%3N) - T0)); break; }
  sleep 0.5
done

WROTE=$(( $(rows) - BEFORE ))
printf '%s\n' "--------------------------------------------------"
printf 'εγγραφές       : %s (στο Kafka %s, στη βάση %s)\n' "$EXPECTED" "$ACCEPTED" "$WROTE"
printf 'HTTP           : %s ms  (%s εγγρ/s)\n' "$T_HTTP" "$((EXPECTED * 1000 / (T_HTTP > 0 ? T_HTTP : 1)))"
printf 'μέχρι τη βάση  : %s ms  (%s εγγρ/s)\n' "$T_DB" "$((WROTE * 1000 / (T_DB > 0 ? T_DB : 1)))"
printf 'σημεία μετρικών: %s\n' "$(points)"
[ "$WROTE" -lt "$EXPECTED" ] && printf 'ΠΡΟΣΟΧΗ: λείπουν %s εγγραφές\n' "$((EXPECTED - WROTE))"
printf '%s\n' "--------------------------------------------------"
