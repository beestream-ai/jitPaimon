/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package ai.beestream.jitpaimon;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;

import uk.ac.manchester.tornado.cudf.enums.CudfType;
import uk.ac.manchester.tornado.cudf.enums.ParquetColumnType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Where each column of a Paimon key-value data file lives on the device, for the read, the merge and
 * the write.
 *
 * <p>A file holds the key columns, {@code _SEQUENCE_NUMBER}, {@code _VALUE_KIND}, then the value
 * columns. Every key column and the sequence number are read as INT64 into long slots {@code 0..nk},
 * so the sort runs over them in place. {@code _VALUE_KIND} is int slot 0.
 */
final class KvLayout {

    /** Buffer kinds, as the read and write lay them out. */
    static final int INTS = 0;

    static final int LONGS = 1;
    static final int DOUBLES = 2;

    /** Values per output column in {@link #gatherSpec}. */
    static final int SPEC_WIDTH = 6;

    /**
     * How a TINYINT is written: INT8 where the device writer has it (TornadoVM's cuDF shim with the
     * INT8 type), else plain INT32 -- a file Paimon reads, but whose schema differs from Paimon's own.
     */
    static final ParquetColumnType TINYINT_WRITE = tinyintWrite();

    private static ParquetColumnType tinyintWrite() {
        try {
            return ParquetColumnType.valueOf("INT8");
        } catch (IllegalArgumentException e) {
            return ParquetColumnType.INT32;
        }
    }

    final RowType keyType;
    final RowType valueType;
    final RowType writeRowType;
    final int keyCount;

    // The read: numeric columns in request order, then the string columns.
    final int[] readColumns;
    final CudfType[] readTypes;
    final int[] stringColumns;
    final int intSlots;
    final int longSlots;
    final int doubleSlots;

    // The write: every file column, in file order.
    final String names;
    final int[] fieldIds;
    final ParquetColumnType[] writeTypes;
    final boolean[] optional;
    final int[] details;
    final int outIntSlots;
    final int outLongSlots;
    final int outDoubleSlots;

    /** Per numeric output column: src buffer, src slot, dst buffer, dst slot, read validity, written index. */
    final int[] gatherSpec;

    final int numericOutputs;

    private KvLayout(RowType keyType, RowType valueType) {
        this.keyType = keyType;
        this.valueType = valueType;
        this.writeRowType = KeyValue.schema(keyType, valueType);
        this.keyCount = keyType.getFieldCount();

        List<DataField> fields = writeRowType.getFields();
        List<Integer> reads = new ArrayList<>();
        List<CudfType> readTypeList = new ArrayList<>();
        List<Integer> strings = new ArrayList<>();
        // Per file column: where the read put it (buffer, slot, read index), -1 for a string.
        int[][] placed = new int[fields.size()][];
        int ints = 0;
        int longs = 0;
        int doubles = 0;
        // Keys and the sequence number first, so they are long slots 0..nk.
        for (int c = 0; c <= keyCount; c++) {
            placed[c] = new int[] {LONGS, longs++, reads.size()};
            reads.add(c);
            readTypeList.add(CudfType.INT64);
        }
        placed[keyCount + 1] = new int[] {INTS, ints++, reads.size()};
        reads.add(keyCount + 1);
        readTypeList.add(CudfType.INT32);
        for (int c = keyCount + 2; c < fields.size(); c++) {
            DataTypeRoot root = fields.get(c).type().getTypeRoot();
            switch (root) {
                case INTEGER, DATE -> {
                    placed[c] = new int[] {INTS, ints++, reads.size()};
                    reads.add(c);
                    readTypeList.add(CudfType.INT32);
                }
                case BIGINT -> {
                    placed[c] = new int[] {LONGS, longs++, reads.size()};
                    reads.add(c);
                    readTypeList.add(CudfType.INT64);
                }
                case DOUBLE -> {
                    placed[c] = new int[] {DOUBLES, doubles++, reads.size()};
                    reads.add(c);
                    readTypeList.add(CudfType.FLOAT64);
                }
                case VARCHAR, CHAR -> {
                    placed[c] = null;
                    strings.add(c);
                }
                default -> throw new IllegalArgumentException("Unsupported column " + fields.get(c));
            }
        }
        this.readColumns = reads.stream().mapToInt(Integer::intValue).toArray();
        this.readTypes = readTypeList.toArray(new CudfType[0]);
        this.stringColumns = strings.stream().mapToInt(Integer::intValue).toArray();
        this.intSlots = ints;
        this.longSlots = longs;
        this.doubleSlots = doubles;

        int numericReads = readColumns.length;
        this.fieldIds = new int[fields.size()];
        this.writeTypes = new ParquetColumnType[fields.size()];
        this.optional = new boolean[fields.size()];
        this.details = new int[fields.size()];
        StringBuilder joined = new StringBuilder();
        List<int[]> spec = new ArrayList<>();
        int outInts = 0;
        int outLongs = 0;
        int outDoubles = 0;
        int stringIndex = 0;
        for (int c = 0; c < fields.size(); c++) {
            DataField field = fields.get(c);
            fieldIds[c] = field.id();
            optional[c] = field.type().isNullable();
            if (c > 0) {
                joined.append('\n');
            }
            joined.append(field.name());
            if (placed[c] == null) {
                writeTypes[c] = ParquetColumnType.STRING;
                // Its validity as the string read wrote it, after the numeric columns'.
                details[c] = numericReads + stringIndex++;
                continue;
            }
            DataTypeRoot root = field.type().getTypeRoot();
            int dst;
            int dstSlot;
            switch (root) {
                case INTEGER -> {
                    writeTypes[c] = ParquetColumnType.INT32;
                    dst = INTS;
                    dstSlot = outInts++;
                }
                case TINYINT -> {
                    // _VALUE_KIND: INT32 annotated INT(8), as Paimon writes it.
                    writeTypes[c] = TINYINT_WRITE;
                    dst = INTS;
                    dstSlot = outInts++;
                }
                case DATE -> {
                    writeTypes[c] = ParquetColumnType.DATE;
                    dst = INTS;
                    dstSlot = outInts++;
                }
                case BIGINT -> {
                    writeTypes[c] = ParquetColumnType.INT64;
                    dst = LONGS;
                    dstSlot = outLongs++;
                }
                case DOUBLE -> {
                    writeTypes[c] = ParquetColumnType.FLOAT64;
                    dst = DOUBLES;
                    dstSlot = outDoubles++;
                }
                default -> throw new IllegalArgumentException("Unsupported column " + field);
            }
            spec.add(
                    new int[] {
                        placed[c][0], placed[c][1], dst, dstSlot, optional[c] ? placed[c][2] : -1, c
                    });
        }
        this.names = joined.toString();
        this.outIntSlots = outInts;
        this.outLongSlots = outLongs;
        this.outDoubleSlots = outDoubles;
        this.numericOutputs = spec.size();
        this.gatherSpec = new int[spec.size() * SPEC_WIDTH];
        for (int o = 0; o < spec.size(); o++) {
            System.arraycopy(spec.get(o), 0, gatherSpec, o * SPEC_WIDTH, SPEC_WIDTH);
        }
    }

    int validColumns() {
        return readColumns.length + stringColumns.length;
    }

    /**
     * The layout for a table whose compactions the device can run exactly as Paimon's default
     * rewriter does, or null with the reason in {@code why}.
     */
    @Nullable
    static KvLayout of(
            RowType keyType, RowType valueType, CoreOptions options, StringBuilder why) {
        if (options.mergeEngine() != CoreOptions.MergeEngine.DEDUPLICATE) {
            why.append("merge-engine ").append(options.mergeEngine());
            return null;
        }
        if (!options.sequenceField().isEmpty()) {
            why.append("sequence.field");
            return null;
        }
        if (options.ignoreDelete()) {
            why.append("ignore-delete");
            return null;
        }
        if (options.recordLevelExpireTime() != null) {
            why.append("record-level.expire-time");
            return null;
        }
        if (options.dataFileThinMode()) {
            why.append("data-file.thin-mode");
            return null;
        }
        if (!options.fileFormatString().toLowerCase(Locale.ROOT).equals("parquet")
                || !options.fileFormatPerLevel().isEmpty()) {
            why.append("file format other than parquet");
            return null;
        }
        if (!options.fileCompressionPerLevel().isEmpty() || codec(options.fileCompression()) < 0) {
            why.append("compression ").append(options.fileCompression());
            return null;
        }
        if (!options.statsModePerLevel().isEmpty()) {
            why.append("metadata.stats-mode.per.level");
            return null;
        }
        if (options.dataFileExternalPaths() != null) {
            why.append("data-file.external-paths");
            return null;
        }
        if (!options.toConfiguration().keySet().stream()
                .filter(k -> k.startsWith("file-index."))
                .toList()
                .isEmpty()) {
            why.append("file-index");
            return null;
        }
        Set<DataTypeRoot> keyRoots = Set.of(DataTypeRoot.INTEGER, DataTypeRoot.BIGINT);
        for (DataField field : keyType.getFields()) {
            if (!keyRoots.contains(field.type().getTypeRoot())) {
                why.append("key column ").append(field);
                return null;
            }
        }
        Set<DataTypeRoot> valueRoots =
                Set.of(
                        DataTypeRoot.INTEGER,
                        DataTypeRoot.BIGINT,
                        DataTypeRoot.DOUBLE,
                        DataTypeRoot.DATE,
                        DataTypeRoot.VARCHAR,
                        DataTypeRoot.CHAR);
        for (DataField field : valueType.getFields()) {
            if (!valueRoots.contains(field.type().getTypeRoot())) {
                why.append("value column ").append(field);
                return null;
            }
        }
        return new KvLayout(keyType, valueType);
    }

    /** The device writer's codec for Paimon's {@code file.compression}, or -1 when it has none. */
    static int codec(String compression) {
        return switch (compression.toLowerCase(Locale.ROOT)) {
            case "zstd" -> 2;
            case "snappy" -> 1;
            case "none", "uncompressed" -> 0;
            default -> -1;
        };
    }
}
