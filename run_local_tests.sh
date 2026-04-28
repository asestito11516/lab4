#!/usr/bin/env bash
set -u

ROOT="$(cd "$(dirname "$0")" && pwd)"
SRC="$ROOT/src"
TESTDIR="$SRC/testfiles"
OUTDIR="$SRC/out"
LOGDIR="$SRC/testlogs"

SENDER_PORT=5001
RECEIVER_PORT=5002
HOST=127.0.0.1

mkdir -p "$TESTDIR" "$OUTDIR" "$LOGDIR"

echo "==> Compiling"
javac "$SRC"/*.java || exit 1

echo "==> Creating test files"
python3 - <<'PY'
from pathlib import Path
base = Path("src/testfiles")
base.mkdir(parents=True, exist_ok=True)

(base / "empty.txt").write_bytes(b"")
(base / "onebyte.txt").write_bytes(b"a")
(base / "mtu_minus_1.bin").write_bytes(b"a" * 999)
(base / "mtu_exact.bin").write_bytes(b"a" * 1000)
(base / "mtu_plus_1.bin").write_bytes(b"a" * 1001)
(base / "medium.bin").write_bytes((b"abcdef1234567890" * 4096))
(base / "large.bin").write_bytes((b"XYZ12345" * 200000))
PY

cleanup() {
  jobs -p | xargs -r kill 2>/dev/null || true
}
trap cleanup EXIT

run_one() {
  local name="$1"
  local infile="$2"
  local mtu="$3"
  local sws="$4"

  local outfile="$OUTDIR/${name}.out"
  local recvlog="$LOGDIR/${name}.receiver.log"
  local sendlog="$LOGDIR/${name}.sender.log"

  rm -f "$outfile" "$recvlog" "$sendlog"

  echo
  echo "===== TEST: $name ====="
  echo "file=$(basename "$infile") mtu=$mtu sws=$sws"

  (
    cd "$SRC" && \
    java TCPend -p "$RECEIVER_PORT" -f "$outfile" -m "$mtu" -c "$sws"
  ) >"$recvlog" 2>&1 &
  local recv_pid=$!

  sleep 0.5

  (
    cd "$SRC" && \
    java TCPend -p "$SENDER_PORT" -s "$HOST" -a "$RECEIVER_PORT" -f "$infile" -m "$mtu" -c "$sws"
  ) >"$sendlog" 2>&1
  local sender_status=$?

  wait "$recv_pid"
  local receiver_status=$?

  if [[ $sender_status -ne 0 ]]; then

    echo "FAIL: sender exited with $sender_status"

    echo "--- sender log ---"

    tail -n 80 "$sendlog"

    echo "--- receiver log ---"

    tail -n 80 "$recvlog"

    kill "$recv_pid" 2>/dev/null || true

    wait "$recv_pid" 2>/dev/null || true

    return 1

  fi

  if [[ $receiver_status -ne 0 ]]; then
    echo "FAIL: receiver exited with $receiver_status"
    echo "--- receiver log ---"
    tail -n 40 "$recvlog"
    return 1
  fi

  if [[ ! -f "$outfile" ]]; then
    echo "FAIL: output file missing"
    return 1
  fi

  local inhash outhash
  inhash="$(sha256sum "$infile" | awk '{print $1}')"
  outhash="$(sha256sum "$outfile" | awk '{print $1}')"

  if [[ "$inhash" != "$outhash" ]]; then
    echo "FAIL: hash mismatch"
    echo "in : $inhash"
    echo "out: $outhash"
    echo "--- sender log ---"
    tail -n 30 "$sendlog"
    echo "--- receiver log ---"
    tail -n 30 "$recvlog"
    return 1
  fi

  echo "PASS"
  echo "sha256=$inhash"
  return 0
}

pass_count=0
fail_count=0

run_test() {
  if run_one "$@"; then
    pass_count=$((pass_count + 1))
  else
    fail_count=$((fail_count + 1))
  fi
}

run_test "t1_empty"       "$TESTDIR/empty.txt"        200   1
run_test "t2_onebyte"     "$TESTDIR/onebyte.txt"     1000   4
run_test "t3_mtu_minus_1" "$TESTDIR/mtu_minus_1.bin" 1000   4
run_test "t4_mtu_exact"   "$TESTDIR/mtu_exact.bin"   1000   4
run_test "t5_mtu_plus_1"  "$TESTDIR/mtu_plus_1.bin"  1000   4
run_test "t6_medium_s"    "$TESTDIR/medium.bin"       200   8
run_test "t7_large_w1"    "$TESTDIR/large.bin"       1000   1
run_test "t8_large_w8"    "$TESTDIR/large.bin"       1400   8

echo
echo "=============================="
echo "Passed: $pass_count"
echo "Failed: $fail_count"
echo "Logs:   $LOGDIR"
echo "Output: $OUTDIR"
echo "=============================="

if [[ $fail_count -ne 0 ]]; then
  exit 1
fi
