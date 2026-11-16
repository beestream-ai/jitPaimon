/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package ai.beestream.jitpaimon.bench;

import ai.beestream.jitpaimon.GpuCompactRewriterFactory;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Generates a primary-key table with overlapping sorted runs, runs a full compaction of every bucket
 * with Paimon's rewriter or the GPU one, and checks the table's contents.
 *
 * <pre>
 * generate --warehouse W --rows N --rounds R --buckets B [--update 0.3] [--delete 0.02]
 * compact  --warehouse W --engine cpu|gpu --writers T
 * checksum --warehouse W
 * </pre>
 */
public final class CompactBench {

    private static final Identifier TABLE = Identifier.create("bench", "kv");
    private static final String USER = "jitpaimon-bench";

    public static void main(String[] args) throws Exception {
        Map<String, String> opt = new HashMap<>();
        for (int i = 1; i < args.length; i += 2) {
            opt.put(args[i].replaceFirst("^--", ""), args[i + 1]);
        }
        String warehouse = opt.get("warehouse");
        switch (args[0]) {
            case "generate" -> generate(warehouse, opt);
            case "append" -> append(warehouse, opt);
            case "compact" -> compact(warehouse, opt);
            case "checksum" -> System.out.println(checksum(table(warehouse)));
            case "files" -> files(table(warehouse));
            default -> throw new IllegalArgumentException("Unknown command " + args[0]);
        }
    }

    static FileStoreTable table(String warehouse) throws Exception {
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(warehouse)));
        return (FileStoreTable) catalog.getTable(TABLE);
    }

    static void generate(String warehouse, Map<String, String> opt) throws Exception {
        long rows = Long.parseLong(opt.getOrDefault("rows", "10000000"));
        int rounds = Integer.parseInt(opt.getOrDefault("rounds", "5"));
        int buckets = Integer.parseInt(opt.getOrDefault("buckets", "8"));
        double update = Double.parseDouble(opt.getOrDefault("update", "0.3"));
        double delete = Double.parseDouble(opt.getOrDefault("delete", "0.02"));
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(warehouse)));
        catalog.createDatabase("bench", true);
        Schema schema =
                Schema.newBuilder()
                        .column("k", DataTypes.BIGINT().notNull())
                        .column("qty", DataTypes.INT())
                        .column("price", DataTypes.DOUBLE())
                        .column("ts", DataTypes.BIGINT())
                        .column("flag", DataTypes.INT())
                        .column("comment", DataTypes.STRING())
                        .primaryKey("k")
                        .option("bucket", Integer.toString(buckets))
                        .option("file.format", "parquet")
                        .option("file.compression", "zstd")
                        .option("write-only", "true")
                        .option("target-file-size", "512 mb")
                        .build();
        catalog.createTable(TABLE, schema, false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(TABLE);
        SplittableRandom random = new SplittableRandom(42);
        long t0 = System.nanoTime();
        for (int round = 0; round < rounds; round++) {
            try (TableWriteImpl<?> write = table.newWrite(USER);
                    TableCommitImpl commit = table.newCommit(USER)) {
                long written = 0;
                for (long i = 0; i < rows; i++) {
                    if (round > 0 && random.nextDouble() >= update) {
                        continue;
                    }
                    long key = round == 0 ? i : random.nextLong(rows);
                    if (round > 0 && random.nextDouble() < delete) {
                        write.write(GenericRow.ofKind(RowKind.DELETE, key, 0, 0.0, 0L, 0, null));
                    } else {
                        write.write(row(key, round, random));
                    }
                    written++;
                }
                commit.commit(round, write.prepareCommit(true, round));
                System.out.printf("round %d: %,d records%n", round, written);
            }
        }
        System.out.printf(
                "generated %,d keys x %d rounds in %.1f s%n",
                rows, rounds, (System.nanoTime() - t0) / 1e9);
    }

    /** More update rounds into an existing table, from {@code --seed}, without compacting. */
    static void append(String warehouse, Map<String, String> opt) throws Exception {
        long rows = Long.parseLong(opt.getOrDefault("rows", "10000000"));
        int rounds = Integer.parseInt(opt.getOrDefault("rounds", "1"));
        double update = Double.parseDouble(opt.getOrDefault("update", "0.3"));
        double delete = Double.parseDouble(opt.getOrDefault("delete", "0.02"));
        FileStoreTable table = table(warehouse);
        SplittableRandom random = new SplittableRandom(Long.parseLong(opt.getOrDefault("seed", "7")));
        long id = table.snapshotManager().latestSnapshotId() + 1;
        for (int round = 0; round < rounds; round++, id++) {
            try (TableWriteImpl<?> write = table.newWrite(USER);
                    TableCommitImpl commit = table.newCommit(USER)) {
                long written = 0;
                for (long i = 0; i < rows; i++) {
                    if (random.nextDouble() >= update) {
                        continue;
                    }
                    long key = random.nextLong(rows);
                    if (random.nextDouble() < delete) {
                        write.write(GenericRow.ofKind(RowKind.DELETE, key, 0, 0.0, 0L, 0, null));
                    } else {
                        write.write(row(key, 100 + round, random));
                    }
                    written++;
                }
                commit.commit(id, write.prepareCommit(true, id));
                System.out.printf("append %d: %,d records%n", round, written);
            }
        }
    }

    private static GenericRow row(long key, int round, SplittableRandom random) {
        int qty = random.nextInt(1, 51);
        double price = Math.round(random.nextDouble(1, 100000) * 100) / 100.0;
        String comment =
                random.nextInt(10) == 0
                        ? null
                        : "r" + round + " " + String.format("%016x", random.nextLong()).substring(0, 1 + random.nextInt(16));
        return GenericRow.of(
                key,
                random.nextInt(20) == 0 ? null : qty,
                price,
                1_700_000_000_000L + random.nextLong(1_000_000_000L),
                round,
                comment == null ? null : BinaryString.fromString(comment));
    }

    static void compact(String warehouse, Map<String, String> opt) throws Exception {
        String engine = opt.getOrDefault("engine", "cpu");
        int writers = Integer.parseInt(opt.getOrDefault("writers", "1"));
        boolean full = Boolean.parseBoolean(opt.getOrDefault("full", "true"));
        Map<String, String> dynamic = new HashMap<>();
        dynamic.put("write-only", "false");
        if (opt.containsKey("trigger")) {
            dynamic.put("num-sorted-run.compaction-trigger", opt.get("trigger"));
        }
        FileStoreTable table = table(warehouse).copy(dynamic);
        int buckets = table.coreOptions().bucket();
        GpuCompactRewriterFactory gpu =
                engine.equals("gpu") ? new GpuCompactRewriterFactory(table) : null;
        ExecutorService pool = Executors.newFixedThreadPool(writers);
        List<Future<List<CommitMessage>>> parts = new ArrayList<>();
        long t0 = System.nanoTime();
        for (int w = 0; w < writers; w++) {
            int first = w;
            parts.add(
                    pool.submit(
                            () -> {
                                try (IOManagerImpl io =
                                                new IOManagerImpl(
                                                        System.getProperty("java.io.tmpdir"));
                                        TableWriteImpl<?> write = table.newWrite(USER)) {
                                    write.withIOManager(io);
                                    if (gpu != null) {
                                        write.withCompactRewriterFactory(gpu);
                                    }
                                    for (int b = first; b < buckets; b += writers) {
                                        write.compact(BinaryRow.EMPTY_ROW, b, full);
                                    }
                                    return write.prepareCommit(true, Long.MAX_VALUE - 1);
                                }
                            }));
        }
        List<CommitMessage> messages = new ArrayList<>();
        for (Future<List<CommitMessage>> part : parts) {
            messages.addAll(part.get());
        }
        long compacted = System.nanoTime();
        try (TableCommitImpl commit = table.newCommit(USER)) {
            commit.commit(Long.MAX_VALUE - 1, messages);
        }
        pool.shutdown();
        if (gpu != null) {
            System.out.println(gpu.stats());
            gpu.close();
        }
        System.out.printf(
                "compact engine=%s writers=%d buckets=%d: %.3f s (+commit %.3f s)%n",
                engine,
                writers,
                buckets,
                (compacted - t0) / 1e9,
                (System.nanoTime() - compacted) / 1e9);
    }

    /** The latest snapshot's data files, bucket by bucket, without names, sizes or times. */
    static void files(FileStoreTable table) {
        org.apache.paimon.types.RowType keyType =
                org.apache.paimon.types.RowType.of(org.apache.paimon.types.DataTypes.BIGINT());
        List<String> lines = new ArrayList<>();
        for (org.apache.paimon.table.source.DataSplit split :
                table.newSnapshotReader().read().dataSplits()) {
            for (org.apache.paimon.io.DataFileMeta f : split.dataFiles()) {
                lines.add(
                        String.format(
                                "bucket=%d level=%d rows=%d key=[%d,%d] seq=[%d,%d] deletes=%s"
                                        + " keyStats=%s valueStats=%s statsCols=%s source=%s",
                                split.bucket(),
                                f.level(),
                                f.rowCount(),
                                f.minKey().getLong(0),
                                f.maxKey().getLong(0),
                                f.minSequenceNumber(),
                                f.maxSequenceNumber(),
                                f.deleteRowCount().orElse(-1L),
                                f.keyStats().toRow().toString(),
                                stats(f.valueStats(), f.valueStatsCols(), table),
                                f.valueStatsCols(),
                                f.fileSource().orElse(null)));
            }
        }
        Collections.sort(lines);
        lines.forEach(System.out::println);
    }

    private static String stats(
            org.apache.paimon.stats.SimpleStats s, List<String> cols, FileStoreTable table) {
        org.apache.paimon.types.RowType valueType = table.rowType();
        StringBuilder b = new StringBuilder("{");
        for (int i = 0; i < s.minValues().getFieldCount(); i++) {
            org.apache.paimon.types.DataType type =
                    cols == null
                            ? valueType.getTypeAt(i)
                            : valueType.getField(cols.get(i)).type();
            InternalRow.FieldGetter getter = InternalRow.createFieldGetter(type, i);
            b.append(zero(getter.getFieldOrNull(s.minValues())))
                    .append("..")
                    .append(zero(getter.getFieldOrNull(s.maxValues())))
                    .append(" nulls=")
                    .append(s.nullCounts().getLong(i))
                    .append("; ");
        }
        return b.append('}').toString();
    }

    /**
     * -0.0 printed as 0.0. libcudf writes a zero minimum as -0.0, as the Parquet format recommends
     * (PARQUET-1222), where parquet-mr writes 0.0; both bound the same values, so the comparison
     * ignores the sign of a zero bound.
     */
    private static Object zero(Object value) {
        if (value instanceof Double d && d == 0.0) {
            return 0.0;
        }
        if (value instanceof Float f && f == 0.0f) {
            return 0.0f;
        }
        return value;
    }

    /** Order-independent digest of the live rows: count, then a sum of per-row hashes. */
    static String checksum(FileStoreTable table) throws Exception {
        ReadBuilder rb = table.newReadBuilder();
        long count = 0;
        long digest = 0;
        try (RecordReader<InternalRow> reader =
                        rb.newRead().createReader(rb.newScan().plan());
                RecordReaderIterator<InternalRow> it = new RecordReaderIterator<>(reader)) {
            while (it.hasNext()) {
                InternalRow r = it.next();
                long h = r.getLong(0);
                h = h * 31 + (r.isNullAt(1) ? -1 : r.getInt(1));
                h = h * 31 + Double.hashCode(r.getDouble(2));
                h = h * 31 + Long.hashCode(r.getLong(3));
                h = h * 31 + r.getInt(4);
                h = h * 31 + (r.isNullAt(5) ? -7 : r.getString(5).toString().hashCode());
                digest += h * 0x9E3779B97F4A7C15L;
                count++;
            }
        }
        return String.format("rows=%d digest=%016x", count, digest);
    }

    private CompactBench() {}
}
