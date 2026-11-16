#!/usr/bin/env bash
# Compacts copies of one table with Paimon's rewriter and the GPU one, step by step, and requires the
# same rows and the same data-file metadata after every step:
#   1. full compaction of fresh files (deletes dropped)
#   2. more rounds, then non-full compactions (deletes kept; inputs include GPU-written files)
#   3. more rounds, then a full compaction
#   4. Paimon's rewriter over the GPU table: it must read the GPU's files
# usage: scripts/verify.sh [rows] [buckets]
set -euo pipefail
cd "$(dirname "$0")/.."
ROWS=${1:-1000000}
BUCKETS=${2:-4}
W=${VERIFY_DIR:-$HOME/paimon-data/verify}
rm -rf "$W"; mkdir -p "$W"
b() { scripts/bench.sh "$@" 2>&1 | grep -v -e '^WARN' -e SLF4J; }
g() { scripts/bench.sh --gpu "$@" 2>&1 | grep -v -e '^WARN' -e SLF4J; }
same() {
  local step=$1
  local c g
  c=$(b checksum --warehouse "$W/cpu" | tail -1)
  g=$(b checksum --warehouse "$W/gpu" | tail -1)
  b files --warehouse "$W/cpu" | grep bucket= > "$W/files-cpu-$step.txt"
  b files --warehouse "$W/gpu" | grep bucket= > "$W/files-gpu-$step.txt"
  if [[ "$c" != "$g" ]]; then echo "FAIL $step rows: cpu $c gpu $g"; exit 1; fi
  if ! diff -q "$W/files-cpu-$step.txt" "$W/files-gpu-$step.txt" >/dev/null; then
    echo "FAIL $step metadata:"; diff "$W/files-cpu-$step.txt" "$W/files-gpu-$step.txt" | head -6; exit 1
  fi
  echo "OK   $step: $c, $(wc -l < "$W/files-cpu-$step.txt") files"
}

b generate --warehouse "$W/cpu" --rows "$ROWS" --rounds 3 --buckets "$BUCKETS" | tail -1
cp -r "$W/cpu" "$W/gpu"

b compact --warehouse "$W/cpu" --engine cpu --writers "$BUCKETS" | tail -1
g compact --warehouse "$W/gpu" --engine gpu --writers "$BUCKETS" | tail -2
same 1-full

# Trigger 2 makes Paimon pick level-1 outputs too, which keep their retractions (dropDelete false).
for i in 1 2 3; do
  for t in cpu gpu; do b append --warehouse "$W/$t" --rows "$ROWS" --rounds 3 --seed $((40 + i)) | tail -1; done
  b compact --warehouse "$W/cpu" --engine cpu --writers "$BUCKETS" --full false --trigger 2 | tail -1
  g compact --warehouse "$W/gpu" --engine gpu --writers "$BUCKETS" --full false --trigger 2 | tail -2
  same "2-partial-$i"
done

for t in cpu gpu; do b append --warehouse "$W/$t" --rows "$ROWS" --rounds 2 --seed 23 | tail -1; done
b compact --warehouse "$W/cpu" --engine cpu --writers "$BUCKETS" | tail -1
g compact --warehouse "$W/gpu" --engine gpu --writers "$BUCKETS" | tail -2
same 3-full

for t in cpu gpu; do b append --warehouse "$W/$t" --rows "$ROWS" --rounds 1 --seed 31 | tail -1; done
b compact --warehouse "$W/cpu" --engine cpu --writers "$BUCKETS" | tail -1
b compact --warehouse "$W/gpu" --engine cpu --writers "$BUCKETS" | tail -1
same 4-cpu-over-gpu-files
echo "ALL OK"
