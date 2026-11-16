#!/usr/bin/env bash
# Runs CompactBench. CPU runs use a plain JDK 21; GPU runs use the TornadoVM SDK's argfile.
# usage: scripts/bench.sh [--gpu] <command> [--key value ...]
set -euo pipefail
cd "$(dirname "$0")/.."
# Versioned on purpose: an inherited TORNADOVM_HOME is often sdkman's `current`, which follows the
# last install (here an old 7.2.0 SDK whose cuDF shim lacks the Parquet reader).
JAVA_HOME=${JITPAIMON_JAVA_HOME:-$HOME/.sdkman/candidates/java/21.0.2-open}
TORNADOVM_HOME=${JITPAIMON_TORNADOVM_HOME:-$HOME/.sdkman/candidates/tornadovm/8.1.0-jdk21-cuda}
export TORNADOVM_HOME TORNADO_SDK=$TORNADOVM_HOME
CP=target/classes:$(cat target/classpath.txt)
HEAP=${HEAP:-24g}
if [[ "${1:-}" == "--gpu" ]]; then
  shift
  CUDF_LD=${CUDF_LD:-$(cat "$HOME/iceberg-gpu/cudf-root-cu13/ld_library_path")}
  export LD_LIBRARY_PATH="$CUDF_LD:${LD_LIBRARY_PATH:-}"
  # native/libtornado-cudf.so, when present: the shim rebuilt with INT8/INT16 widening on read
  # (Paimon's _VALUE_KIND is an INT_8 column), searched before the SDK's.
  LIBPATH="$TORNADOVM_HOME/lib"
  [[ -f native/libtornado-cudf.so ]] && LIBPATH="$PWD/native:$LIBPATH"
  # native/patch: ParquetColumnType with INT8/INT16, which that shim writes (Paimon's _VALUE_KIND).
  PATCH=()
  [[ -d native/patch ]] && PATCH=(--patch-module "tornado.cudf=$PWD/native/patch")
  exec "$JAVA_HOME/bin/java" "-Xmx$HEAP" @"$TORNADOVM_HOME/tornado-argfile" "${PATCH[@]}" \
    -Djava.library.path="$LIBPATH" -Dtornado.device.memory=${DEVICE_MEMORY:-16GB} -cp "$CP" ${MAIN:-ai.beestream.jitpaimon.bench.CompactBench} "$@"
fi
exec "$JAVA_HOME/bin/java" "-Xmx$HEAP" --enable-preview -cp "$CP" ${MAIN:-ai.beestream.jitpaimon.bench.CompactBench} "$@"
