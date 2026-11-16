package ai.beestream.jitpaimon.bench;

import uk.ac.manchester.tornado.api.TaskGraph;
import uk.ac.manchester.tornado.api.TornadoExecutionPlan;
import uk.ac.manchester.tornado.api.enums.DataTransferMode;
import uk.ac.manchester.tornado.api.types.arrays.ByteArray;
import uk.ac.manchester.tornado.api.types.arrays.DoubleArray;
import uk.ac.manchester.tornado.api.types.arrays.IntArray;
import uk.ac.manchester.tornado.api.types.arrays.LongArray;
import uk.ac.manchester.tornado.cudf.Cudf;
import uk.ac.manchester.tornado.cudf.enums.CudfType;

/** Checks that cuDF reads a Paimon KV file's columns, including the INT_8 _VALUE_KIND, as asked. */
public final class ReadProbe {
    public static void main(String[] args) throws Exception {
        int rows = 0;
        for (String a : args) {
            long[] meta = Cudf.parquetMetadata(a);
            rows += (int) meta[0];
            System.out.printf("%s rows=%d rowGroups=%d columns=%d%n", a.substring(a.lastIndexOf('/') + 1), meta[0], meta[1], meta[2]);
        }
        String path = String.join("\n", args);
        // _KEY_k, _SEQUENCE_NUMBER, _VALUE_KIND, k, qty, price, ts, flag
        int[] columns = {0, 1, 2, 3, 4, 5, 6, 7};
        CudfType[] types = {
            CudfType.INT64, CudfType.INT64, CudfType.INT32, CudfType.INT64,
            CudfType.INT32, CudfType.FLOAT64, CudfType.INT64, CudfType.INT32
        };
        IntArray ints = new IntArray(3 * rows);
        LongArray longs = new LongArray(4 * rows);
        DoubleArray doubles = new DoubleArray(rows);
        ByteArray valid = new ByteArray(8 * rows);
        StringBuilder holder = new StringBuilder(path);
        TaskGraph g =
                new TaskGraph("probe")
                        .libraryTask("read", Cudf::readParquetColumns, holder, 0, 0, columns, types,
                                new long[] {rows}, rows, ints, longs, doubles, valid, true)
                        .transferToHost(DataTransferMode.EVERY_EXECUTION, ints, longs, doubles, valid);
        try (TornadoExecutionPlan plan = new TornadoExecutionPlan(g.snapshot())) {
            plan.execute();
        }
        java.util.Set<Long> distinct = new java.util.HashSet<>();
        long badKeys = 0;
        for (int i = 0; i < rows; i++) {
            distinct.add(longs.get(i));
            if (longs.get(i) != longs.get(2 * rows + i)) {
                badKeys++;
            }
        }
        System.out.printf("total=%d distinctKeys=%d keyMismatch(_KEY_k!=k)=%d%n", rows, distinct.size(), badKeys);
        for (int i = 0; i < 3; i++) {
            System.out.printf("key=%d seq=%d kind=%d k=%d qty=%d(valid %d) price=%.2f ts=%d flag=%d%n",
                    longs.get(i), longs.get(rows + i), ints.get(i), longs.get(2 * rows + i),
                    ints.get(rows + i), valid.get(4 * rows + i), doubles.get(i), longs.get(3 * rows + i),
                    ints.get(2 * rows + i));
        }
        int deletes = 0;
        for (int i = 0; i < rows; i++) {
            if (ints.get(i) == 3) {
                deletes++;
            }
        }
        System.out.println("deletes=" + deletes);
    }
}
