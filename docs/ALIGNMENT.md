# Community alignment: where jitPaimon plugs into Apache Paimon

Checked 2026-11-16 against apache/paimon master 00b4d05 and release 2.1.0.

## Hook used

`TableWrite.withCompactRewriterFactory(CompactRewriterFactory)`. Added by apache/paimon#9762
("[core] Allow custom primary-key compaction rewriters", jordepic, merged 2026-09-14) and shipped in
Paimon **2.1.0** (`org/apache/paimon/mergetree/compact/CompactRewriterFactory.class` is in
`paimon-core-2.1.0.jar` on Maven Central). The PR was opened for exactly this use: "An external engine
currently has to reconstruct the writer setup to replace the file read/merge/write work with its own
implementation." The PR's author plans to use it from DataFusion/StreamFusion (Rust). The maintainer's
review (JingsongLi): "Requirement fit: SUPPORTED. Implementation: CLEAN."

Contract (`docs/docs/program-api/java-writing.md`): Paimon keeps input selection, scheduling and
commits. A rewriter returns `CompactResult` file changes and must preserve merge, sequence,
changelog, deletion-vector, record-expiration and metadata contracts. It owns and closes the default
rewriter and may delegate any compaction it does not support. It works for primary-key merge-tree
writers only. Write-only mode never calls it.

**No change to Paimon is needed.** jitPaimon is an out-of-tree jar that a Flink/Spark/Java compaction
job installs.

## Scope of the first slice

The GPU rewriter is used only where the default rewriter is a plain `MergeTreeCompactRewriter`:
`changelog-producer` none/input, no deletion vectors, no lookup, `merge-engine = deduplicate`, no
sequence field, Parquet, flat primitive and string columns, no record-level expiry. Every other case
goes to the default rewriter unchanged.

## Gap found (candidate upstream ask, not filed)

The factory receives only the default rewriter. Paths, file format, the stats collectors and the
`DataFileMeta` builder sit in `KeyValueFileWriterFactory`/`FileReaderFactory`, which are `protected`
fields of `MergeTreeCompactRewriter`. The prototype reads them from a class in the same package. A
small accessor on the rewriter, or a context argument for the factory, would remove that. Bring
this up only once there are numbers, and only with the user's yes.

## Related upstream activity

- apache/paimon#9923 (open): GPU scans of Paimon through Sirius (C++, DuckDB). That covers reads in
  another engine. jitPaimon covers compaction inside Paimon's own JVM writers. They do not overlap.
- apache/paimon#9660 (open): Parquet row-group copy fast path, append-only compaction only.
- apache/paimon#10133 (open): `compaction.task-threads`, more CPU threads per write subtask. This is
  the CPU baseline to beat.
- apache/paimon#11201 (merged 2026-11-16): packs DV materialization. This is the DV path for a later slice.
