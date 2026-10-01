#!/usr/bin/env bash
# Daily atomic backup of the running MySQL database; retain seven days.
set -Eeuo pipefail
umask 077

PROJ=${CSZY_BACKUP_PROJECT_DIR:-/opt/cszy2000w-ultimate}
BACKUP_DIR=${CSZY_BACKUP_DIR:-$PROJ/backups}
DOCKER_BIN=${CSZY_DOCKER_BIN:-/usr/bin/docker}
MYSQL_CONTAINER=${CSZY_MYSQL_CONTAINER:-cszy_mysql}
mkdir -p "$BACKUP_DIR"
chmod 700 "$BACKUP_DIR"
TS=$(date +%F)
OUT="$BACKUP_DIR/cszy2000_${TS}.sql.gz"
TMP=$(mktemp "$BACKUP_DIR/.cszy2000_${TS}.XXXXXX.sql.gz")
trap 'rm -f -- "$TMP"' EXIT

# Read the running container's existing credentials internally; never put the
# password in host command arguments, logs, or a sourced project .env file.
if ! "$DOCKER_BIN" exec "$MYSQL_CONTAINER" sh -c '
    : "${MYSQL_ROOT_PASSWORD:?MYSQL_ROOT_PASSWORD is missing}"
    : "${MYSQL_DATABASE:?MYSQL_DATABASE is missing}"
    MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysqldump -uroot \
        --single-transaction --quick --routines --events --triggers \
        --no-tablespaces "$MYSQL_DATABASE"
' 2>"$BACKUP_DIR/last_error.log" | gzip > "$TMP"; then
    echo "BACKUP FAILED: database export/compression failed; see $BACKUP_DIR/last_error.log" >&2
    exit 1
fi
if ! gzip -t "$TMP" || ! gzip -cd "$TMP" | tail -n 10 | grep -q '^-- Dump completed on '; then
    echo "BACKUP FAILED: incomplete dump; existing backup preserved" >&2
    exit 1
fi
mv -f -- "$TMP" "$OUT"
# Rotation only occurs after a validated, successful export.
find "$BACKUP_DIR" -type f -name 'cszy2000_*.sql.gz' -mtime +6 -delete
echo "OK $OUT $(du -h "$OUT" | cut -f1)"
