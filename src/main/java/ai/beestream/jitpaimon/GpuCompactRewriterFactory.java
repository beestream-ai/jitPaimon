/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package ai.beestream.jitpaimon;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.mergetree.compact.CompactRewriter;
import org.apache.paimon.mergetree.compact.CompactRewriterFactory;
import org.apache.paimon.mergetree.compact.JitPaimonAccess;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.StatsCollectorFactories;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Installs the device rewriter on the buckets of a table it supports; every other bucket, and every
 * compaction the device cannot take, keeps Paimon's rewriter.
 *
 * <pre>{@code
 * try (GpuCompactRewriterFactory gpu = new GpuCompactRewriterFactory(table)) {
 *     write.withCompactRewriterFactory(gpu);
 *     ...
 * }
 * }</pre>
 *
 * <p>{@code jitpaimon.max-batch-rows} (system property, default 64M) caps the rows of one device
 * batch.
 */
public final class GpuCompactRewriterFactory implements CompactRewriterFactory, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(GpuCompactRewriterFactory.class);

    private final FileStoreTable table;
    private final CoreOptions options;
    private final long maxBatchRows = Long.getLong("jitpaimon.max-batch-rows", 64L << 20);
    private final GpuMergeTreeCompactRewriter.Stats stats = new GpuMergeTreeCompactRewriter.Stats();
    private DeviceCompactor device;
    private KvLayout layout;
    private Optional<SimpleStatsExtractor> extractor;
    private boolean checked;

    public GpuCompactRewriterFactory(FileStoreTable table) {
        this.table = table;
        this.options = table.coreOptions();
    }

    @Override
    public synchronized CompactRewriter create(
            BinaryRow partition, int bucket, CompactRewriter defaultRewriter) {
        if (!JitPaimonAccess.isPlainMergeTreeRewriter(defaultRewriter)) {
            stats.cpu("rewriter " + defaultRewriter.getClass().getSimpleName());
            return defaultRewriter;
        }
        KeyValueFileWriterFactory writerFactory = JitPaimonAccess.writerFactory(defaultRewriter);
        if (!checked) {
            checked = true;
            StringBuilder why = new StringBuilder();
            layout = KvLayout.of(writerFactory.keyType(), writerFactory.valueType(), options, why);
            if (layout == null) {
                LOG.info("Table {} compacts on the CPU: {}", table.name(), why);
                stats.cpu(why.toString());
            } else {
                RowType writeRowType = KeyValue.schema(layout.keyType, layout.valueType);
                extractor =
                        FileFormat.fromIdentifier("parquet", options.toConfiguration())
                                .createStatsExtractor(
                                        writeRowType,
                                        StatsCollectorFactories.createStatsFactories(
                                                options.statsMode(),
                                                options,
                                                writeRowType.getFieldNames()));
                device = new DeviceCompactor(layout);
            }
        }
        if (layout == null) {
            return defaultRewriter;
        }
        return new GpuMergeTreeCompactRewriter(
                defaultRewriter,
                writerFactory,
                layout,
                device,
                options,
                table.schema().id(),
                extractor,
                maxBatchRows,
                stats);
    }

    /** What the rewriters made by this factory did so far. */
    public String stats() {
        return stats.toString();
    }

    @Override
    public synchronized void close() {
        if (device != null) {
            device.close();
            device = null;
        }
    }
}
