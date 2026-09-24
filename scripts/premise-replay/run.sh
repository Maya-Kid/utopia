#!/usr/bin/env bash
# 前提回放（#875）的一条命令：构建 server，起在一个一次性的空库上，跑 replay.mjs，收尾。
#
# 需要：cargo、node ≥ 18、psql、curl。REPLAY_DATABASE_URL 指向一个空的、只给这次用的库——
# 脚本不建库也不删库，库里已经有表就拒绝跑，免得连上开发库或生产库。server 的 worker 会消费
# 这个库里的任务，所以它也不能同时给别的服务或测试用。
#
# 用法：REPLAY_DATABASE_URL=postgres://user:pass@host:5432/empty_db scripts/premise-replay/run.sh
# 可选：REPLAY_PORT（缺省 18751）、REPLAY_OUT（结果目录）、REPLAY_SCHEDULER=0（跳过约十分钟的
# 定时推导那一段）。结果：$REPLAY_OUT/{trace.jsonl,summary.json,server.log}
set -euo pipefail
cd "$(dirname "$0")/../.."

: "${REPLAY_DATABASE_URL:?set REPLAY_DATABASE_URL to an empty, disposable database}"
PORT="${REPLAY_PORT:-18751}"
OUT="${REPLAY_OUT:-$PWD/target/premise-replay/$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "$OUT" "$OUT/restart"
DATA_DIR="$(mktemp -d)"
SERVER_PID=""
cleanup() {
  if [ -n "$SERVER_PID" ]; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  rm -rf "$DATA_DIR"
}
trap cleanup EXIT

# 连接串拆成 libpq 的环境变量：口令不进命令行，也不进日志
eval "$(node -e '
  const u = new URL(process.env.REPLAY_DATABASE_URL);
  const q = (s) => JSON.stringify(s);
  console.log(`export PGHOST=${q(u.hostname)} PGPORT=${q(u.port || "5432")} PGUSER=${q(decodeURIComponent(u.username))} PGPASSWORD=${q(decodeURIComponent(u.password))} PGDATABASE=${q(u.pathname.slice(1))}`);
')"
tables="$(psql -X -tAc "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'")"
if [ "$tables" != "0" ]; then
  echo "refusing to run: $PGDATABASE already has $tables tables; point REPLAY_DATABASE_URL at an empty database" >&2
  exit 2
fi

echo "--- build utopia-server"
cargo build --locked -q -p utopia-server
BIN="${CARGO_TARGET_DIR:-target}/debug/utopia-server"

start_server() {
  echo "--- server start $(date -u +%FT%TZ)" >>"$OUT/server.log"
  UTOPIA_DATABASE_URL="$REPLAY_DATABASE_URL" UTOPIA_BIND_ADDR="127.0.0.1:$PORT" \
    UTOPIA_DATA_DIR="$DATA_DIR" RUST_LOG="${RUST_LOG:-info}" "$BIN" >>"$OUT/server.log" 2>&1 &
  SERVER_PID=$!
  # 就绪轮询：成败看健康检查，不看等了多久；60 秒还没起来就算失败
  for _ in $(seq 1 120); do
    curl -sf "http://127.0.0.1:$PORT/api/v1/health" >/dev/null 2>&1 && return 0
    kill -0 "$SERVER_PID" 2>/dev/null || break
    sleep 0.5
  done
  echo "server did not become healthy; see $OUT/server.log" >&2
  return 1
}

echo "--- start server on 127.0.0.1:$PORT"
start_server

echo "--- replay"
REPLAY_BASE="http://127.0.0.1:$PORT" REPLAY_OUT="$OUT" REPLAY_RESTART_DIR="$OUT/restart" \
  node scripts/premise-replay/replay.mjs &
HARNESS_PID=$!
# 回放中途要一次服务重启：它写 request，这里重启 server 后写 done（同步点是文件，不是等待时长）
while kill -0 "$HARNESS_PID" 2>/dev/null; do
  if [ -f "$OUT/restart/request" ]; then
    rm -f "$OUT/restart/request"
    kill "$SERVER_PID" && wait "$SERVER_PID" 2>/dev/null || true
    start_server
    touch "$OUT/restart/done"
  fi
  sleep 0.2
done
status=0
wait "$HARNESS_PID" || status=$?
echo "--- replay exited with $status; results in $OUT"
exit "$status"
