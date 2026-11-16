# jitPaimon

Apache Paimon primary-key compaction on NVIDIA GPUs, with [TornadoVM](https://github.com/beehive-lab/TornadoVM) and cuDF.

jitPaimon is a `CompactRewriterFactory`, the hook that Paimon 2.1.0 opens for other compaction engines
(apache/paimon#9762). Install it on a table write and Paimon keeps choosing what to compact, when, and
how to commit. jitPaimon only replaces the merge itself:

```java
try (GpuCompactRewriterFactory gpu = new GpuCompactRewriterFactory(table)) {
    write.withCompactRewriterFactory(gpu);
    write.compact(partition, bucket, true);
    ...
}
```

Paimon itself needs no change. [docs/ALIGNMENT.md](docs/ALIGNMENT.md) covers the upstream hook and the
upstream issues and PRs it relates to.

## What runs on the device

For each batch of sections (key ranges that do not overlap):

1. cuDF reads the batch's Parquet files. Key columns and `_SEQUENCE_NUMBER` go into INT64 slots.
2. cuDF sorts the rows by key, then by sequence number.
3. A kernel keeps the last row of each key, and drops retractions when the output level drops deletes.
4. cuDF selects the survivors, a kernel gathers their columns, and cuDF writes one Parquet file.
5. A kernel reduces the sequence range, the retraction count and the first and last keys.

The `DataFileMeta` (key and value stats, `valueStatsCols`, ranges, delete count) is built from the
written footer by Paimon's own Parquet stats extractor, as Paimon's writer builds it.

**Supported:** deduplicate merge engine; no `sequence.field`, `ignore-delete`, record-level expiry,
thin mode, deletion vectors, lookup changelog or file index; Parquet with zstd, snappy or no
compression; INT/BIGINT keys; INT, BIGINT, DOUBLE, DATE and STRING values; local files. Anything
else goes to Paimon's rewriter.

**Fallback:** a compaction also goes to Paimon's rewriter when any of the following holds:
- it does not fit on the device;
- its input files' schemas differ;
- the device fails;
- the result fails its consistency checks.

After a failure the device plan is rebuilt.

## Correctness

`scripts/verify.sh` compacts two copies of a table, one with Paimon's rewriter and one with jitPaimon,
and requires the same rows (count and order-independent digest) and the same data-file metadata after
every step:

- a full compaction;
- three rounds of partial compactions, including level-1 outputs that keep retractions;
- a full compaction over GPU-written and Paimon-written files together;
- Paimon's rewriter over the GPU-written files.

All steps pass. There is one difference, and it is deliberate. When a DOUBLE column's minimum is a zero,
libcudf writes it as `-0.0`, as the Parquet format recommends (PARQUET-1222), where parquet-mr writes
`0.0`. Both bound the same values.

## Results

Full compaction of every bucket: 50M keys, a base round plus 3 rounds of 30% upserts with 2% deletes;
93.3M records in 800 files into 49.4M rows, in 32 buckets. The table has six columns (BIGINT key, INT,
DOUBLE, BIGINT, INT, STRING) and is Parquet with zstd.

Setup: RTX 4090, 32-core host, JDK 21, Paimon 2.1.0, TornadoVM 8.1.0 (CUDA) with the shim fixes below.
`writers` are concurrent table writes, as compaction subtasks in Flink would be; each run started from
the same files in the page cache, and each configuration ran once.

| Engine | Writers | Time | vs Paimon, 1 writer | vs Paimon's best |
|---|---:|---:|---:|---:|
| Paimon | 1 | 45.81 s | 1x | |
| Paimon | 8 | 8.20 s | 5.6x | |
| Paimon | 16 | **6.58 s** | 7.0x | 1x |
| Paimon | 32 | 8.48 s | 5.4x | |
| jitPaimon | 1 | **3.78 s** | **12.1x** | **1.74x** |
| jitPaimon | 4 | 3.96 s | 11.6x | 1.66x |
| jitPaimon | 32 | 4.27 s | 10.7x | 1.54x |

The final table is the same in every case (`rows=49406207 digest=6f92096044e5d274`). The jitPaimon
times include compiling the kernels on the first batch. One writer is enough for the GPU, so the
other cores stay free for ingestion and queries. More writers add only lock contention on the one
device.

Reproduce:

```bash
mvn package -DskipTests
scripts/bench.sh generate --warehouse ~/paimon-data/b50/base --rows 50000000 --rounds 4 --buckets 32
scripts/run-bench.sh ~/paimon-data/b50/base ~/paimon-data/b50/runs "cpu:1 cpu:16 gpu:1"
```

## TornadoVM

jitPaimon runs with TornadoVM 8.1.0 plus three fixes to its cuDF shim, which are not upstream yet
(branch `fix/cudf-read-narrow-ints`):

- **Reads widen INT(8)/INT(16) columns.** Paimon's `_VALUE_KIND` is one, and it was refused.
- **Writes INT8/INT16.** GPU-written files then have Paimon's schema.
- **Multi-file reads fall back to one read per file**, concatenated, when libcudf refuses files whose
  schemas differ only by writer (root name, converted types).

`scripts/bench.sh` picks up the rebuilt `native/libtornado-cudf.so` and the patched
`ParquetColumnType` from `native/`.

Requirements: JDK 21, the TornadoVM SDK `8.1.0-jdk21-cuda`, and RAPIDS libcudf 26.08 (CUDA 13).
`scripts/bench.sh` pins the versioned SDK on purpose. An inherited `TORNADOVM_HOME` is often sdkman's
`current`, which may be an older SDK.

## Next

- Bound output files by `target-file-size`. Today each batch writes one file.
- Support deletion-vector and lookup tables (`LookupMergeTreeCompactRewriter`).
- Run compaction from Flink: a `CompactAction` with the factory installed.
- Benchmark on A100/H100 and on object stores.
