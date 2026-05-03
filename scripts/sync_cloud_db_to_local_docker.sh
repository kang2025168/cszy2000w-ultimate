#!/usr/bin/env bash
set -euo pipefail

# 把云服务器 MySQL 数据库复制到本地 Docker MySQL。
#
# 默认行为：
# - 从云端导出整个数据库。
# - 导入到本地 Docker 暴露的 MySQL 端口。
# - 会覆盖本地同名表，所以必须显式设置 CONFIRM=YES。
#
# 示例：
#   CONFIRM=YES ./scripts/sync_cloud_db_to_local_docker.sh
#
# 只同步部分表：
#   CONFIRM=YES TABLES="stock_operations option_spreads option_spread_legs" \
#     ./scripts/sync_cloud_db_to_local_docker.sh
#
# 常用覆盖参数：
#   REMOTE_DB_HOST=138.197.75.51
#   REMOTE_DB_PORT=3307
#   REMOTE_DB_USER=tradebot
#   REMOTE_DB_PASS='TradeBot#2026!'
#   REMOTE_DB_NAME=cszy2000
#   LOCAL_DB_HOST=127.0.0.1
#   LOCAL_DB_PORT=3307
#   LOCAL_DB_USER=tradebot
#   LOCAL_DB_PASS='TradeBot#2026!'
#   LOCAL_DB_NAME=cszy2000

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -f ".env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source ".env"
  set +a
fi

REMOTE_DB_HOST="${REMOTE_DB_HOST:-138.197.75.51}"
REMOTE_DB_PORT="${REMOTE_DB_PORT:-3307}"
REMOTE_DB_USER="${REMOTE_DB_USER:-${DB_USER:-tradebot}}"
REMOTE_DB_PASS="${REMOTE_DB_PASS:-${DB_PASS:-}}"
REMOTE_DB_NAME="${REMOTE_DB_NAME:-${DB_NAME:-cszy2000}}"

LOCAL_DB_HOST="${LOCAL_DB_HOST:-127.0.0.1}"
LOCAL_DB_PORT="${LOCAL_DB_PORT:-3307}"
LOCAL_DB_USER="${LOCAL_DB_USER:-${DB_USER:-tradebot}}"
LOCAL_DB_PASS="${LOCAL_DB_PASS:-${DB_PASS:-}}"
LOCAL_DB_NAME="${LOCAL_DB_NAME:-${DB_NAME:-cszy2000}}"

CONFIRM="${CONFIRM:-NO}"
TABLES="${TABLES:-}"
KEEP_DUMP="${KEEP_DUMP:-0}"

if [[ "$CONFIRM" != "YES" ]]; then
  cat <<EOF
[ABORT] 这个脚本会把云端数据库导入本地 Docker MySQL，并覆盖本地同名表。

云端: ${REMOTE_DB_USER}@${REMOTE_DB_HOST}:${REMOTE_DB_PORT}/${REMOTE_DB_NAME}
本地: ${LOCAL_DB_USER}@${LOCAL_DB_HOST}:${LOCAL_DB_PORT}/${LOCAL_DB_NAME}

确认执行请加：
  CONFIRM=YES ./scripts/sync_cloud_db_to_local_docker.sh

只同步部分表可以加：
  TABLES="stock_operations option_spreads option_spread_legs"
EOF
  exit 1
fi

if ! command -v mysqldump >/dev/null 2>&1; then
  echo "[FATAL] 找不到 mysqldump。请先安装 MySQL client。"
  exit 1
fi

if ! command -v mysql >/dev/null 2>&1; then
  echo "[FATAL] 找不到 mysql。请先安装 MySQL client。"
  exit 1
fi

if [[ -z "$REMOTE_DB_PASS" || -z "$LOCAL_DB_PASS" ]]; then
  echo "[FATAL] REMOTE_DB_PASS 或 LOCAL_DB_PASS 为空。请检查 .env 或显式传入。"
  exit 1
fi

mkdir -p "$ROOT_DIR/data/db-sync"
STAMP="$(date +%Y%m%d_%H%M%S)"
DUMP_FILE="$ROOT_DIR/data/db-sync/cloud_${REMOTE_DB_NAME}_${STAMP}.sql"

echo "[INFO] remote=${REMOTE_DB_USER}@${REMOTE_DB_HOST}:${REMOTE_DB_PORT}/${REMOTE_DB_NAME}"
echo "[INFO] local =${LOCAL_DB_USER}@${LOCAL_DB_HOST}:${LOCAL_DB_PORT}/${LOCAL_DB_NAME}"
if [[ -n "$TABLES" ]]; then
  echo "[INFO] tables=${TABLES}"
else
  echo "[INFO] tables=ALL"
fi
echo "[INFO] dump_file=${DUMP_FILE}"

echo "[STEP 1/3] 检查本地 Docker MySQL 是否可连接..."
mysql \
  -h"${LOCAL_DB_HOST}" \
  -P"${LOCAL_DB_PORT}" \
  -u"${LOCAL_DB_USER}" \
  -p"${LOCAL_DB_PASS}" \
  -e "SELECT VERSION() AS local_mysql_version;" >/dev/null

echo "[STEP 2/3] 从云端导出..."
if [[ -n "$TABLES" ]]; then
  # shellcheck disable=SC2086
  mysqldump \
    -h"${REMOTE_DB_HOST}" \
    -P"${REMOTE_DB_PORT}" \
    -u"${REMOTE_DB_USER}" \
    -p"${REMOTE_DB_PASS}" \
    --single-transaction \
    --quick \
    --routines \
    --triggers \
    --events \
    --set-gtid-purged=OFF \
    --default-character-set=utf8mb4 \
    "${REMOTE_DB_NAME}" ${TABLES} > "${DUMP_FILE}"
else
  mysqldump \
    -h"${REMOTE_DB_HOST}" \
    -P"${REMOTE_DB_PORT}" \
    -u"${REMOTE_DB_USER}" \
    -p"${REMOTE_DB_PASS}" \
    --single-transaction \
    --quick \
    --routines \
    --triggers \
    --events \
    --set-gtid-purged=OFF \
    --default-character-set=utf8mb4 \
    "${REMOTE_DB_NAME}" > "${DUMP_FILE}"
fi

echo "[STEP 3/3] 导入本地 Docker MySQL..."
mysql \
  -h"${LOCAL_DB_HOST}" \
  -P"${LOCAL_DB_PORT}" \
  -u"${LOCAL_DB_USER}" \
  -p"${LOCAL_DB_PASS}" \
  -e "CREATE DATABASE IF NOT EXISTS \`${LOCAL_DB_NAME}\` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"

mysql \
  -h"${LOCAL_DB_HOST}" \
  -P"${LOCAL_DB_PORT}" \
  -u"${LOCAL_DB_USER}" \
  -p"${LOCAL_DB_PASS}" \
  "${LOCAL_DB_NAME}" < "${DUMP_FILE}"

echo "[OK] 同步完成。"
echo "[OK] 本地库: ${LOCAL_DB_HOST}:${LOCAL_DB_PORT}/${LOCAL_DB_NAME}"

if [[ "$KEEP_DUMP" == "1" ]]; then
  echo "[OK] 保留 dump: ${DUMP_FILE}"
else
  rm -f "${DUMP_FILE}"
  echo "[OK] 已删除临时 dump。若想保留，下次加 KEEP_DUMP=1。"
fi




#CONFIRM=YES ./scripts/sync_cloud_db_to_local_docker.sh
