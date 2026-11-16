/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package ai.beestream.jitpaimon;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.format.parquet.ParquetUtil;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.mergetree.compact.AbstractCompactRewriter;
import org.apache.paimon.mergetree.compact.CompactRewriter;
import org.apache.paimon.shade.org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.paimon.shade.org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.paimon.shade.org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.stats.SimpleStatsConverter;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * A deduplicating merge-tree compaction on the device. Each batch of sections (key intervals that do
 * not overlap) becomes one file; a compaction the device cannot take, or one that fails there, is
 * handed to Paimon's rewriter whole.
 */
final class GpuMergeTreeCompactRewriter extends AbstractCompactRewriter {

    private static final Logger LOG = LoggerFactory.getLogger(GpuMergeTreeCompactRewriter.class);

    private final CompactRewriter fallback;
    private final KeyValueFileWriterFactory writerFactory;
    private final KvLayout layout;
    private final DeviceCompactor device;
    private final CoreOptions options;
    private final long schemaId;
    private final Optional<SimpleStatsExtractor> stats;
    private final SimpleStatsConverter keyStats;
    private final SimpleStatsConverter valueStats;
    private final long maxBatchRows;
    private final Stats counters;

    GpuMergeTreeCompactRewriter(
            CompactRewriter fallback,
            KeyValueFileWriterFactory writerFactory,
            KvLayout layout,
            DeviceCompactor device,
            CoreOptions options,
            long schemaId,
            Optional<SimpleStatsExtractor> stats,
            long maxBatchRows,
            Stats counters) {
        this.fallback = fallback;
        this.writerFactory = writerFactory;
        this.layout = layout;
        this.device = device;
        this.options = options;
        this.schemaId = schemaId;
        this.stats = stats;
        this.keyStats = new SimpleStatsConverter(layout.keyType);
        this.valueStats = new SimpleStatsConverter(layout.valueType, options.statsDenseStore());
        this.maxBatchRows = maxBatchRows;
        this.counters = counters;
    }

    @Override
    public CompactResult rewrite(int outputLevel, boolean dropDelete, List<List<SortedRun>> sections)
            throws Exception {
        List<DataFileMeta> before = extractFilesFromSections(sections);
        String reason = unsupported(before, sections);
        if (reason != null) {
            LOG.debug("Compaction on the CPU: {}", reason);
            counters.cpu(reason);
            return fallback.rewrite(outputLevel, dropDelete, sections);
        }
        FileIO io = writerFactory.getFileIO();
        DataFilePathFactory outputs = writerFactory.pathFactory(outputLevel);
        List<DataFileMeta> after = new ArrayList<>();
        List<Path> written = new ArrayList<>();
        try {
            for (List<List<SortedRun>> batch : batches(sections)) {
                List<DataFileMeta> files = extractFilesFromSections(batch);
                List<String> inputs = new ArrayList<>(files.size());
                long rows = 0;
                for (DataFileMeta file : files) {
                    inputs.add(local(writerFactory.pathFactory(file.level()).toPath(file)));
                    rows += file.rowCount();
                }
                long chars = stringBytes(io, files);
                Path output = outputs.newPath();
                written.add(output);
                long t0 = System.nanoTime();
                DeviceCompactor.Summary summary =
                        device.compact(
                                inputs,
                                rows,
                                chars,
                                local(output),
                                dropDelete,
                                KvLayout.codec(options.fileCompression()));
                counters.device(rows, summary.rows(), System.nanoTime() - t0);
                check(files, rows, summary);
                if (Boolean.getBoolean("jitpaimon.debug")) {
                    StringBuilder b = new StringBuilder();
                    for (DataFileMeta f : files) {
                        b.append(String.format(" L%d:%d:%s", f.level(), f.rowCount(), f.fileName().substring(0, 13)));
                    }
                    System.err.printf(
                            "[jitpaimon] level=%d drop=%b sections=%d rows=%d -> %s inputs:%s%n",
                            outputLevel, dropDelete, batch.size(), rows, summary, b);
                }
                if (summary.rows() == 0) {
                    io.deleteQuietly(output);
                    continue;
                }
                after.add(meta(io, output, outputLevel, summary));
            }
        } catch (MixedSchemas mixed) {
            for (Path path : written) {
                io.deleteQuietly(path);
            }
            LOG.debug("Compaction on the CPU: {}", mixed.getMessage());
            counters.cpu(mixed.getMessage());
            return fallback.rewrite(outputLevel, dropDelete, sections);
        } catch (Exception | Error failure) {
            for (Path path : written) {
                io.deleteQuietly(path);
            }
            LOG.warn("Device compaction failed; Paimon's rewriter runs it instead", failure);
            counters.cpu("device failure: " + failure);
            return fallback.rewrite(outputLevel, dropDelete, sections);
        }
        return new CompactResult(before, after);
    }

    private String unsupported(List<DataFileMeta> files, List<List<SortedRun>> sections) {
        for (DataFileMeta file : files) {
            if (file.schemaId() != schemaId) {
                return "file of schema " + file.schemaId() + ", table at " + schemaId;
            }
            if (!file.fileName().endsWith(".parquet")) {
                return "file " + file.fileName() + " is not Parquet";
            }
            if (file.externalPath().isPresent()) {
                return "external path";
            }
        }
        for (List<SortedRun> section : sections) {
            if (rows(section) > maxBatchRows) {
                return "a section of " + rows(section) + " rows";
            }
        }
        return null;
    }

    /**
     * What any correct merge of the files satisfies, checked before a batch's file is kept: no more
     * rows than were read; sequence numbers within the inputs' range; and, since a file of a sorted
     * run holds each key once, at least as many keys as the largest file, less the retractions that
     * could have removed some.
     */
    private static void check(List<DataFileMeta> files, long rows, DeviceCompactor.Summary summary) {
        long largest = 0;
        long retractions = 0;
        long minSeq = Long.MAX_VALUE;
        long maxSeq = Long.MIN_VALUE;
        for (DataFileMeta file : files) {
            largest = Math.max(largest, file.rowCount());
            retractions += file.deleteRowCount().orElse(file.rowCount());
            minSeq = Math.min(minSeq, file.minSequenceNumber());
            maxSeq = Math.max(maxSeq, file.maxSequenceNumber());
        }
        boolean ok = summary.rows() <= rows && summary.rows() + retractions >= largest;
        if (summary.rows() > 0) {
            ok &= summary.minSequence() >= minSeq && summary.maxSequence() <= maxSeq;
        }
        if (!ok) {
            throw new IllegalStateException(
                    String.format(
                            "Device result fails its checks: %d of %d rows (largest file %d, %d retractions),"
                                    + " sequence [%d, %d] for inputs [%d, %d]",
                            summary.rows(), rows, largest, retractions,
                            summary.minSequence(), summary.maxSequence(), minSeq, maxSeq));
        }
    }

    /** Consecutive sections, up to {@code maxBatchRows} rows a batch. */
    private List<List<List<SortedRun>>> batches(List<List<SortedRun>> sections) {
        List<List<List<SortedRun>>> batches = new ArrayList<>();
        List<List<SortedRun>> current = new ArrayList<>();
        long rows = 0;
        for (List<SortedRun> section : sections) {
            long sectionRows = rows(section);
            if (!current.isEmpty() && rows + sectionRows > maxBatchRows) {
                batches.add(current);
                current = new ArrayList<>();
                rows = 0;
            }
            current.add(section);
            rows += sectionRows;
        }
        if (!current.isEmpty()) {
            batches.add(current);
        }
        return batches;
    }

    private static long rows(List<SortedRun> section) {
        long rows = 0;
        for (SortedRun run : section) {
            for (DataFileMeta file : run.files()) {
                rows += file.rowCount();
            }
        }
        return rows;
    }

    /**
     * An upper bound on any one string column's bytes across the files (its uncompressed chunks),
     * after checking that every file has the same Parquet schema: the device reads a batch's files
     * as one table, and files whose schemas differ are not read correctly that way.
     */
    private long stringBytes(FileIO io, List<DataFileMeta> files) throws IOException {
        long[] perColumn = new long[layout.stringColumns.length];
        String schema = null;
        for (DataFileMeta file : files) {
            Path path = writerFactory.pathFactory(file.level()).toPath(file);
            try (ParquetFileReader reader =
                    ParquetUtil.getParquetReader(io, path, file.fileSize(), options.toConfiguration())) {
                // The fields only: the root's name (cuDF's "schema", Paimon's "paimon_schema") is not read.
                String fileSchema =
                        reader.getFooter().getFileMetaData().getSchema().getFields().toString();
                if (schema == null) {
                    schema = fileSchema;
                } else if (!schema.equals(fileSchema)) {
                    throw new MixedSchemas(file.fileName());
                }
                for (BlockMetaData block : reader.getFooter().getBlocks()) {
                    List<ColumnChunkMetaData> chunks = block.getColumns();
                    for (int s = 0; s < perColumn.length; s++) {
                        perColumn[s] += chunks.get(layout.stringColumns[s]).getTotalUncompressedSize();
                    }
                }
            }
        }
        return Arrays.stream(perColumn).max().orElse(0);
    }

    private DataFileMeta meta(FileIO io, Path path, int level, DeviceCompactor.Summary summary)
            throws IOException {
        long size = io.getFileSize(path);
        SimpleColStats[] rowStats;
        if (stats.isPresent()) {
            rowStats = stats.get().extract(io, path, size);
        } else {
            rowStats = new SimpleColStats[layout.writeRowType.getFieldCount()];
            Arrays.fill(rowStats, SimpleColStats.NONE);
        }
        int keys = layout.keyCount;
        SimpleStats key = keyStats.toBinaryAllMode(Arrays.copyOfRange(rowStats, 0, keys));
        Pair<List<String>, SimpleStats> value =
                valueStats.toBinary(Arrays.copyOfRange(rowStats, keys + 2, rowStats.length));
        return DataFileMeta.create(
                path.getName(),
                size,
                summary.rows(),
                keyRow(summary.minKey()),
                keyRow(summary.maxKey()),
                key,
                value.getValue(),
                summary.minSequence(),
                summary.maxSequence(),
                schemaId,
                level,
                Collections.emptyList(),
                summary.retractions(),
                null,
                FileSource.COMPACT,
                value.getKey(),
                null,
                null,
                null);
    }

    private BinaryRow keyRow(long[] values) {
        BinaryRow row = new BinaryRow(values.length);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        for (int c = 0; c < values.length; c++) {
            if (layout.keyType.getTypeAt(c).getTypeRoot() == DataTypeRoot.INTEGER) {
                writer.writeInt(c, (int) values[c]);
            } else {
                writer.writeLong(c, values[c]);
            }
        }
        writer.complete();
        return row;
    }

    private static String local(Path path) {
        String scheme = path.toUri().getScheme();
        if (scheme != null && !scheme.equals("file")) {
            throw new UnsupportedOperationException("The device reads and writes local files, not " + path);
        }
        return path.toUri().getPath();
    }

    @Override
    public CompactResult upgrade(int outputLevel, DataFileMeta file) throws Exception {
        return fallback.upgrade(outputLevel, file);
    }

    @Override
    public void close() throws IOException {
        fallback.close();
    }

    /** A batch whose files' Parquet schemas differ. */
    private static final class MixedSchemas extends IOException {
        MixedSchemas(String file) {
            super("file " + file + " has a different Parquet schema from the batch's first file");
        }
    }

    /** What the rewriters of one factory did. */
    static final class Stats {
        private long deviceBatches;
        private long deviceRowsIn;
        private long deviceRowsOut;
        private long deviceNanos;
        private final List<String> cpuReasons = new ArrayList<>();

        synchronized void device(long rowsIn, long rowsOut, long nanos) {
            deviceBatches++;
            deviceRowsIn += rowsIn;
            deviceRowsOut += rowsOut;
            deviceNanos += nanos;
        }

        synchronized void cpu(String reason) {
            cpuReasons.add(reason);
        }

        @Override
        public synchronized String toString() {
            return String.format(
                    "device batches=%d rowsIn=%,d rowsOut=%,d time=%.3f s; cpu compactions=%d %s",
                    deviceBatches,
                    deviceRowsIn,
                    deviceRowsOut,
                    deviceNanos / 1e9,
                    cpuReasons.size(),
                    cpuReasons.isEmpty() ? "" : cpuReasons.subList(0, Math.min(3, cpuReasons.size())));
        }
    }
}
