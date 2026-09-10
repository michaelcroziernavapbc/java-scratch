#!/usr/bin/env bash
#
# Builds a self-contained db2downloader binary for Linux.
#
#   ./build-native.sh              # host architecture (arm64 on Apple Silicon)
#   ./build-native.sh amd64        # Intel/AMD, e.g. an x86-64 EC2 instance
#   ./build-native.sh arm64        # AWS Graviton
#   NATIVE_TEST=1 ./build-native.sh amd64    # run the test suite as a native image instead
#
# GraalVM's native-image cannot cross-compile: the binary's architecture and libc are whatever the
# build host is. So the build runs inside a container of the target platform. Building for the
# host architecture is native speed; building for the other one goes through QEMU emulation and is
# several times slower.
#
# Any arguments after the architecture are passed through to Gradle, e.g.:
#   ./build-native.sh amd64 --info
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE="${NATIVE_BUILD_IMAGE:-ghcr.io/graalvm/native-image-community:21}"

case "${1:-}" in
  amd64|x86_64|x86-64|intel) TARGET_ARCH=amd64; shift ;;
  arm64|aarch64|graviton)    TARGET_ARCH=arm64; shift ;;
  ""|-*)
    case "$(uname -m)" in
      arm64|aarch64) TARGET_ARCH=arm64 ;;
      x86_64|amd64)  TARGET_ARCH=amd64 ;;
      *) echo "error: cannot map host arch '$(uname -m)' to a target; pass amd64 or arm64." >&2; exit 1 ;;
    esac
    ;;
  *)
    echo "error: unknown architecture '$1'. Use amd64 or arm64." >&2
    exit 1
    ;;
esac

# NATIVE_TEST=1 compiles the Hadoop-free tests into a native image and runs them there, which is
# how the parquet write path gets verified ahead-of-time rather than only on the JVM.
if [[ "${NATIVE_TEST:-}" == "1" ]]; then
  GRADLE_TASK="db2downloader:nativeTest"
else
  GRADLE_TASK="db2downloader:nativeCompile"
fi

OUTPUT="db2downloader/build/native/nativeCompile/db2downloader-linux-${TARGET_ARCH}"

# nativeCompile clears its output directory on every run, so building the second architecture would
# otherwise delete the first one's binary. Finished artifacts are copied into dist/, which Gradle
# does not manage, so both architectures can sit side by side.
DIST_DIR="$REPO_ROOT/dist"

# Resolve jdbc_drivers through the symlink and mount the real directory, since a bind mount of the
# repo alone would leave the symlink dangling inside the container.
if [[ ! -d "$REPO_ROOT/db2downloader/jdbc_drivers" ]]; then
  echo "error: db2downloader/jdbc_drivers is missing or dangling." >&2
  echo "       Point it at a directory holding the DB2 JCC jars first." >&2
  exit 1
fi
DRIVERS="$(cd "$REPO_ROOT/db2downloader/jdbc_drivers" && pwd -P)"

HOST_ARCH="$(uname -m)"
echo "Target:  linux/${TARGET_ARCH}"
echo "Task:    ${GRADLE_TASK}"
echo "Drivers: ${DRIVERS}"
case "$HOST_ARCH:$TARGET_ARCH" in
  arm64:arm64|aarch64:arm64|x86_64:amd64|amd64:amd64) ;;
  *) echo "Note:    host is ${HOST_ARCH}, so this runs under QEMU emulation and will be slow." ;;
esac
echo

docker run --rm \
  --platform "linux/${TARGET_ARCH}" \
  --entrypoint /bin/bash \
  -v "$REPO_ROOT:/workspace" \
  -v "$DRIVERS:/drivers:ro" \
  -v "db2downloader-gradle-cache-${TARGET_ARCH}:/root/.gradle" \
  -w /workspace \
  -e GRADLE_TASK="$GRADLE_TASK" \
  -e TARGET_ARCH="$TARGET_ARCH" \
  "$IMAGE" -c '
    set -euo pipefail
    # The GraalVM image is a minimal Oracle Linux with no findutils, and the gradlew launcher
    # shells out to xargs.
    command -v xargs >/dev/null || microdnf install -y --nodocs findutils >/dev/null
    ./gradlew --no-daemon --no-configuration-cache \
      -PjdbcDriversDir=/drivers \
      -PtargetArch="$TARGET_ARCH" \
      "$GRADLE_TASK" "$@"
  ' -- "$@"

echo
if [[ "${NATIVE_TEST:-}" == "1" ]]; then
  echo "Native tests finished for linux/${TARGET_ARCH}."
  exit 0
fi

if [[ ! -f "$REPO_ROOT/$OUTPUT" ]]; then
  echo "error: expected binary at $OUTPUT but it is not there." >&2
  exit 1
fi

mkdir -p "$DIST_DIR"
cp -f "$REPO_ROOT/$OUTPUT" "$DIST_DIR/db2downloader-linux-${TARGET_ARCH}"

echo "Built: dist/db2downloader-linux-${TARGET_ARCH}"
file "$DIST_DIR/db2downloader-linux-${TARGET_ARCH}"
ls -lh "$DIST_DIR/db2downloader-linux-${TARGET_ARCH}" | awk '{print "Size: " $5}'
echo
echo "Binaries available:"
ls -1 "$DIST_DIR" | sed 's|^|  dist/|'
