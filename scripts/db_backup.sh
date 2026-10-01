#!/bin/bash
# DB 每日备份：mysqldump cszy2000 -> backups/，gzip 压缩，保留最近 7 天
# 凭证从项目 .env 读取，不在脚本/crontab 中写明文密码
set -u
PROJ=/opt/cszy2000w-ultimate
BACKUP_DIR=$PROJ/backups
mkdir -p "$BACKUP_DIR"
DB_NAME=$(grep -oP "^DB_NAME=\K.*" "$PROJ/.env" | head -1)
ROOT_PW=$(grep -oP "^MYSQL_ROOT_PASSWORD=\K.*" "$PROJ/.env" | head -1)
TS=$(date +%F)
OUT="$BACKUP_DIR/cszy2000_${TS}.sql.gz"
/usr/bin/docker exec cszy_mysql mysqldump -uroot -p"$ROOT_PW" --single-transaction --routines --events --triggers "$DB_NAME" 2>"$BACKUP_DIR/last_error.log" | gzip > "$OUT"
if [ ! -s "$OUT" ]; then
  echo "BACKUP FAILED: $OUT empty, see $BACKUP_DIR/last_error.log" >&2
  rm -f "$OUT"
  exit 1
fi
# 轮转：删除 7 天前的备份
find "$BACKUP_DIR" -name "cszy2000_*.sql.gz" -mtime +7 -delete
echo "OK $OUT $(du -h "$OUT" | cut -f1)"
