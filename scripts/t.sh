#!/usr/bin/env bash
# Τρέχει tests και τυπώνει ΜΟΝΟ σύνοψη (+ τα σφάλματα όταν υπάρχουν), αντί για εκατοντάδες
# γραμμές maven output. Στόχος: μια απάντηση που διαβάζεται με μια ματιά.
#
#   scripts/t.sh ingestion-service              # ένα module
#   scripts/t.sh api-service BucketsTest        # ένα test
#   scripts/t.sh all                            # όλα (αργό, τα IT θέλουν Docker)
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1
export JAVA_HOME="${JAVA_HOME:-$HOME/.jdks/corretto-21.0.2}"

MODULE="${1:-all}"
TEST="${2:-}"

if [ "$MODULE" = "all" ]; then
  MODULES=(common ingestion-service api-service analytics-ui-service)
  SEL=()
else
  MODULES=("$MODULE")
  SEL=(-o -pl "$MODULE")
fi

# Τα παλιά reports φεύγουν, αλλιώς η σύνοψη μετράει περασμένα τρεξίματα.
for m in "${MODULES[@]}"; do rm -rf "$m/target/surefire-reports"; done

# ΠΡΟΣΟΧΗ, αυτό έχει δαγκώσει: το incremental compile του maven δεν ξαναμεταγλωττίζει test
# sources που δεν άλλαξαν, ακόμα κι όταν άλλαξε η υπογραφή που καλούν. Ένα test με σφάλμα
# μεταγλώττισης έτρεχε με το ΠΑΛΙΟ .class και η σουίτα περνούσε πράσινη. Αν ο κώδικας είναι
# νεότερος από τα μεταγλωττισμένα tests, τα ξαναχτίζουμε από την αρχή.
for m in "${MODULES[@]}"; do
  [ -d "$m/target/test-classes" ] || continue
  if [ -n "$(find "$m/src/main" -newer "$m/target/test-classes" -name '*.java' -print -quit 2>/dev/null)" ]; then
    rm -rf "$m/target/test-classes"
  fi
done

LOG="$(mktemp)"
ARGS=("${SEL[@]}" test)
[ -n "$TEST" ] && ARGS+=("-Dtest=$TEST" -DfailIfNoTests=false)
./mvnw -q "${ARGS[@]}" > "$LOG" 2>&1
STATUS=$?

REPORTS=()
for m in "${MODULES[@]}"; do
  for f in "$m"/target/surefire-reports/*.txt; do [ -f "$f" ] && REPORTS+=("$f"); done
done
if [ ${#REPORTS[@]} -eq 0 ]; then
  echo "κανένα test δεν έτρεξε"
  [ $STATUS -eq 0 ] && exit 0
fi

awk '
  /^Tests run:/ {
    for (i = 1; i <= NF; i++) {
      if ($i ~ /^[0-9]+,?$/) { v = $i + 0; k = $(i-1); gsub(/[:,]/, "", k)
        if (k == "run") run += v; else if (k == "Failures") f += v
        else if (k == "Errors") e += v; else if (k == "Skipped") s += v }
    }
  }
  END { printf "%d tests, %d failures, %d errors, %d skipped\n", run, f, e, s }
' "${REPORTS[@]}" 2>/dev/null

if [ $STATUS -ne 0 ]; then
  echo "--- ΑΠΕΤΥΧΕ (maven exit $STATUS) ---"
  # Οι γραμμές που εξηγούν γιατί: compile errors, assertion failures, stack κορυφές.
  grep -E '^\[ERROR\]|ERROR\].*\.java|expected:|but was:|Caused by:|<<< (FAILURE|ERROR)' "$LOG" \
    | grep -v 'To see the full stack trace' | head -40
  [ ${#REPORTS[@]} -gt 0 ] && grep -lE '<<< (FAILURE|ERROR)' "${REPORTS[@]}" 2>/dev/null | head -5
fi
rm -f "$LOG"
exit $STATUS
