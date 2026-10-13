#!/usr/bin/env bash
# Σηκώνει τα τρία services μαζί. Logs στο logs/<service>.log, σταμάτημα με Ctrl-C.
#   bash run-all.sh            όλα
#   bash run-all.sh ingestion  μόνο ένα
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
export JAVA_HOME="${JAVA_HOME:-$HOME/.jdks/corretto-21.0.2}"
mkdir -p logs
# Το common πρέπει να υπάρχει στο τοπικό repo, αλλιώς τα services δεν τρέχουν ξεχωριστά
echo "-> install του common (μία φορά ανά αλλαγή στο proto)"
./mvnw -q install -DskipTests -pl common -am
services=("${@:-ingestion api analytics-ui}")
declare -A module=( [ingestion]=ingestion-service [api]=api-service [analytics-ui]=analytics-ui-service )
declare -A port=( [ingestion]=8080 [api]=8082 [analytics-ui]=8085 )
pids=()
for s in ${services[@]}; do
  echo "-> $s (${module[$s]}) στο :${port[$s]}  — log: logs/$s.log"
  ./mvnw -q -o -pl "${module[$s]}" spring-boot:run > "logs/$s.log" 2>&1 &
  pids+=($!)
done
trap 'kill ${pids[@]} 2>/dev/null' INT TERM
for s in ${services[@]}; do
  for _ in $(seq 1 60); do
    curl -sf -m 2 "localhost:${port[$s]}/actuator/health" >/dev/null && { echo "   $s UP"; break; }
    sleep 3
  done
done
echo "Έτοιμα. Ctrl-C για σταμάτημα."
wait
