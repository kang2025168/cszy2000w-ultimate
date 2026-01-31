# -*- coding: utf-8 -*-
import os
import sys
import pymysql

def main():
    host = os.getenv("DB_HOST", "mysql")
    port = int(os.getenv("DB_PORT", "3306"))
    user = os.getenv("DB_USER", "tradebot")
    pwd  = os.getenv("DB_PASS", "")
    db   = os.getenv("DB_NAME", "cszy2000")

    print(f"DB={host}:{port}/{db} user={user}", flush=True)

    conn = pymysql.connect(
        host=host, port=port, user=user, password=pwd, database=db,
        charset="utf8mb4", autocommit=True
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        print("OK: mysql ping", flush=True)
    finally:
        conn.close()

if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception as e:
        print(f"FAIL: {type(e).__name__}: {e}", flush=True)
        sys.exit(2)
