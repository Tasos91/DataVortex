#!/usr/bin/env bash
# ΑΠΑΡΑΙΤΗΤΟ μετά από αλλαγή στο .proto ή σε migration: ξανακτίζει και εγκαθιστά το common,
# αλλιώς τα services βλέπουν το παλιό. Χωρίς -o επίτηδες (το install plugin μπορεί να λείπει).
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1
export JAVA_HOME="${JAVA_HOME:-$HOME/.jdks/corretto-21.0.2}"

LOG="$(mktemp)"
./mvnw -q install -DskipTests -pl common -am > "$LOG" 2>&1
STATUS=$?
[ $STATUS -eq 0 ] && echo "ok common (proto + migrations εγκαταστάθηκαν)" \
  || grep -E '^\[ERROR\]' "$LOG" | head -20
rm -f "$LOG"
exit $STATUS
