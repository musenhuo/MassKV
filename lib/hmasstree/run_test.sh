#!/bin/bash
# H-Masstree Standalone Test Runner
# 用法: ./run_test.sh [-v] [-n NUM_KEYS] [-t NUM_THREADS]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BUILD_DIR="$PROJECT_ROOT/build_hmasstree"

echo "========================================"
echo "H-Masstree Test Suite"
echo "========================================"
echo "Project root: $PROJECT_ROOT"
echo "Build dir: $BUILD_DIR"
echo ""

# 检查是否需要构建
if [[ ! -d "$BUILD_DIR" ]]; then
    echo "[INFO] Creating build directory..."
    mkdir -p "$BUILD_DIR"
fi

# 编译测试程序
echo "[INFO] Building hmasstree_test..."
cd "$PROJECT_ROOT"

g++ -std=c++17 -O2 -fpermissive \
    -I lib/hmasstree -I include -I lib \
    -include lib/hmasstree/config.h \
    -o "$BUILD_DIR/hmasstree_test" \
    lib/hmasstree/hmasstree_test.cpp \
    lib/hmasstree/hmasstree_wrapper.cc \
    lib/hmasstree/straccum.cc \
    lib/hmasstree/string.cc \
    lib/hmasstree/str.cc \
    lib/hmasstree/string_slice.cc \
    lib/hmasstree/kvthread.cc \
    lib/hmasstree/misc.cc \
    lib/hmasstree/compiler.cc \
    lib/hmasstree/memdebug.cc \
    lib/hmasstree/clp.c \
    -lpthread

echo "[INFO] Compiled hmasstree_test"

cd "$BUILD_DIR"

# 运行测试
echo ""
echo "[INFO] Running tests..."
echo ""

./hmasstree_test "$@"

exit_code=$?

echo ""
if [[ $exit_code -eq 0 ]]; then
    echo "[SUCCESS] All tests passed!"
else
    echo "[FAILURE] Some tests failed."
fi

exit $exit_code
