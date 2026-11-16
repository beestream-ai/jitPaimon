#!/usr/bin/env bash
# Full compaction of a copy of one table, per engine and writer count; each run starts from the same
# files. The copy leaves the inputs in the page cache for both engines.
# usage: scripts/run-bench.sh <base-warehouse> <out-dir> "cpu:1 cpu:32 gpu:1 gpu:32 ..."
set -euo pipefail
cd "$(dirname "$0")/.."
BASE=$1
OUT=$2
RUNS=$3
mkdir -p "$OUT"
LOG="$OUT/bench.log"
echo "# $(date -Is) base=$BASE $(nproc) cores, $(nvidia-smi --query-gpu=name --format=csv,noheader)" | tee -a "$LOG"
for run in $RUNS; do
  engine=${run%%:*}
  writers=${run##*:}
  W="$OUT/w-$engine-$writers"
  rm -rf "$W"
  cp -r "$BASE" "$W"
  sync
  if [[ $engine == gpu ]]; then flag=--gpu; else flag=; fi
  scripts/bench.sh $flag compact --warehouse "$W" --engine "$engine" --writers "$writers" 2>&1 \
    | grep -E '^(compact|device)' | tee -a "$LOG"
  if [[ "${CHECKSUM:-}" == *"$run"* ]]; then
    echo "checksum $run: $(scripts/bench.sh checksum --warehouse "$W" 2>&1 | tail -1)" | tee -a "$LOG"
  fi
  [[ "${KEEP:-}" == *"$run"* ]] || rm -rf "$W"
done
