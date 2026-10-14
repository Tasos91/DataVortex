#!/usr/bin/env bash
# Compile ενός module με σιωπηλή επιτυχία: τυπώνει "ok <module>" ή μόνο τα σφάλματα.
#   scripts/c.sh ingestion-service
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1
export JAVA_HOME="${JAVA_HOME:-$HOME/.jdks/corretto-21.0.2}"

M="${1:?δώσε module}"
LOG="$(mktemp)"
./mvnw -q -o -pl "$M" compile > "$LOG" 2>&1
STATUS=$?
if [ $STATUS -eq 0 ]; then
  echo "ok $M"
else
  grep -E '^\[ERROR\].*\.java|^\[ERROR\] [A-Za-z]' "$LOG" | head -30
fi
rm -f "$LOG"
exit $STATUS
