#!/usr/bin/env bash
# Sweep writer count and epoch wait; prints one markdown row per run.
# Usage: writers.sh <seconds> <clients> [sqlog bench args...]
set -euo pipefail
secs=$1 clients=$2; shift 2
bin="$(dirname "$0")/../../target/release/sqlog"
echo "| writers | epoch ms | txn/s | p50 ms | p99 ms | max ms | epochs/s | txns/epoch | conflicts/epoch | check |"
echo "|---|---|---|---|---|---|---|---|---|---|"
for w in 1 2 4 6 8 10; do
  for e in 0 5; do
    out=$("$bin" bench --seconds "$secs" --clients "$clients" --writers "$w" --replicas 1 --epoch-ms "$e" "$@" 2>&1 || true)
    tps=$(sed -nE 's/.* ([0-9]+) txn\/s.*/\1/p' <<<"$out")
    read -r p50 p99 mx < <(sed -nE 's/.*p50 ([0-9.]+) +p90 [0-9.]+ +p99 ([0-9.]+) +max ([0-9.]+).*/\1 \2 \3/p' <<<"$out")
    read -r eps tpe conf < <(sed -nE 's/^epochs: [0-9]+ \(([0-9.]+)\/s\), ([0-9]+) txns\/epoch, conflicts ([0-9]+).*/\1 \2 \3/p' <<<"$out")
    epochs=$(sed -nE 's/^epochs: ([0-9]+) .*/\1/p' <<<"$out")
    cpe=$(awk -v c="$conf" -v e="$epochs" 'BEGIN{printf "%.2f", (e>0)?c/e:0}')
    check=$(grep -q PASS <<<"$out" && echo pass || echo FAIL)
    echo "| $w | $e | $tps | $p50 | $p99 | $mx | $eps | $tpe | $cpe | $check |"
  done
done
