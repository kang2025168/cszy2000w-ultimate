from __future__ import annotations

"""数据库自动迁移：补齐旧表字段，并创建持仓展示表。"""

from .config import settings
from .db import db_conn


STOCK_OPERATION_COLUMNS = {
    "current_price": "DECIMAL(18,6) NULL",
    "strategy_group": "VARCHAR(8) NULL",
    "capital_pool": "VARCHAR(16) NULL",
    "margin_used": "TINYINT DEFAULT 0",
    "rebalance_date": "DATE NULL",
    "last_capital_check_at": "DATETIME NULL",
}


def _column_exists(conn, table: str, column: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS n
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = %s
              AND COLUMN_NAME = %s
            """,
            (table, column),
        )
        return int(cur.fetchone()["n"]) > 0


def _varchar_length(conn, table: str, column: str) -> int | None:
    """读取 VARCHAR 字段长度，用于旧表自动升级。"""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT CHARACTER_MAXIMUM_LENGTH AS n
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = %s
              AND COLUMN_NAME = %s
            """,
            (table, column),
        )
        row = cur.fetchone()
        if not row or row.get("n") is None:
            return None
        return int(row["n"])


def ensure_stock_operations_columns() -> None:
    """检查旧交易控制表，缺少 V1 需要的字段就自动添加。"""
    s = settings()
    with db_conn(s) as conn:
        for column, ddl in STOCK_OPERATION_COLUMNS.items():
            if not _column_exists(conn, s.ops_table, column):
                with conn.cursor() as cur:
                    cur.execute(f"ALTER TABLE `{s.ops_table}` ADD COLUMN `{column}` {ddl}")
                print(f"[SCHEMA] added {s.ops_table}.{column}", flush=True)
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE `{s.ops_table}`
                SET strategy_group = stock_type
                WHERE strategy_group IS NULL OR strategy_group = ''
                """
            )


def ensure_position_holdings_table() -> None:
    """创建独立的持仓展示/复盘表，不替代旧的 stock_operations。"""
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS position_holdings (
                  id BIGINT AUTO_INCREMENT PRIMARY KEY,
                  symbol VARCHAR(64) NOT NULL,
                  strategy_group VARCHAR(8) NOT NULL,
                  stock_type VARCHAR(8),
                  status VARCHAR(16) DEFAULT 'open',
                  qty DECIMAL(18,6) DEFAULT 0,
                  avg_entry_price DECIMAL(18,6),
                  current_price DECIMAL(18,6),
                  market_value DECIMAL(18,2),
                  cost_basis DECIMAL(18,2),
                  unrealized_pnl DECIMAL(18,2),
                  unrealized_pnl_pct DECIMAL(10,4),
                  realized_pnl DECIMAL(18,2),
                  entry_time DATETIME,
                  exit_time DATETIME,
                  holding_days INT DEFAULT 0,
                  stop_loss_price DECIMAL(18,6),
                  take_profit_price DECIMAL(18,6),
                  b_stage INT,
                  capital_pool VARCHAR(16),
                  margin_used TINYINT DEFAULT 0,
                  alpaca_position_id VARCHAR(128),
                  last_order_id VARCHAR(128),
                  last_order_side VARCHAR(16),
                  last_update_time DATETIME,
                  notes TEXT,
                  INDEX idx_symbol_strategy_status (symbol, strategy_group, status),
                  INDEX idx_status (status),
                  INDEX idx_strategy_group (strategy_group)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            # 老版本 symbol 是 VARCHAR(16)，遇到 Alpaca 期权/特殊符号会写入失败。
            # 这里启动时自动扩容，不删除数据。
            symbol_len = _varchar_length(conn, "position_holdings", "symbol")
            if symbol_len is not None and symbol_len < 64:
                cur.execute("ALTER TABLE position_holdings MODIFY COLUMN symbol VARCHAR(64) NOT NULL")
                print("[SCHEMA] upgraded position_holdings.symbol to VARCHAR(64)", flush=True)


def ensure_control_state_tables() -> None:
    """创建机器人架构需要的状态表：风控、资金、心跳、命令。"""
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS risk_state (
                  id BIGINT AUTO_INCREMENT PRIMARY KEY,
                  mode VARCHAR(16) NOT NULL,
                  risk_multiplier DECIMAL(10,4) DEFAULT 1,
                  daily_pnl_pct DECIMAL(10,6) DEFAULT 0,
                  loss_days INT DEFAULT 0,
                  max_drawdown_pct DECIMAL(10,6) DEFAULT 0,
                  block_all_new TINYINT DEFAULT 0,
                  block_b_buy TINYINT DEFAULT 0,
                  block_d_buy TINYINT DEFAULT 0,
                  suggest_capital_mode VARCHAR(16),
                  reason VARCHAR(128),
                  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  INDEX idx_updated_at (updated_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS capital_state (
                  id BIGINT AUTO_INCREMENT PRIMARY KEY,
                  strategy_group VARCHAR(8) NOT NULL,
                  target_capital DECIMAL(18,2) DEFAULT 0,
                  used_capital DECIMAL(18,2) DEFAULT 0,
                  available_capital DECIMAL(18,2) DEFAULT 0,
                  risk_adjusted_target DECIMAL(18,2) DEFAULT 0,
                  can_open_new TINYINT DEFAULT 1,
                  reason VARCHAR(128),
                  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  INDEX idx_strategy_group (strategy_group),
                  INDEX idx_updated_at (updated_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS capital_pools (
                  id BIGINT AUTO_INCREMENT PRIMARY KEY,
                  allocation_month DATE NOT NULL,
                  strategy_group VARCHAR(8) NOT NULL,
                  mode VARCHAR(16) NOT NULL,
                  base_percent DECIMAL(10,4) DEFAULT 0,
                  base_target_capital DECIMAL(18,2) DEFAULT 0,
                  total_risk_percent DECIMAL(10,4) DEFAULT 1,
                  pool_risk_percent DECIMAL(10,4) DEFAULT 1,
                  risk_target_capital DECIMAL(18,2) DEFAULT 0,
                  used_capital DECIMAL(18,2) DEFAULT 0,
                  available_capital DECIMAL(18,2) DEFAULT 0,
                  used_percent DECIMAL(10,4) DEFAULT 0,
                  source_equity DECIMAL(18,2) DEFAULT 0,
                  source_buying_power DECIMAL(18,2) DEFAULT 0,
                  notes VARCHAR(255),
                  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  UNIQUE KEY uq_month_group (allocation_month, strategy_group),
                  INDEX idx_month (allocation_month),
                  INDEX idx_strategy_group (strategy_group)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bot_heartbeats (
                  bot_name VARCHAR(64) PRIMARY KEY,
                  status VARCHAR(32) NOT NULL DEFAULT 'unknown',
                  last_seen_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  last_message VARCHAR(255),
                  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bot_commands (
                  id BIGINT AUTO_INCREMENT PRIMARY KEY,
                  bot_name VARCHAR(64) NOT NULL,
                  command VARCHAR(64) NOT NULL,
                  payload JSON,
                  status VARCHAR(32) NOT NULL DEFAULT 'pending',
                  result VARCHAR(255),
                  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  executed_at DATETIME,
                  INDEX idx_bot_status (bot_name, status),
                  INDEX idx_created_at (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bot_controls (
                  bot_name VARCHAR(64) PRIMARY KEY,
                  enabled TINYINT NOT NULL DEFAULT 1,
                  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            for bot_name in ("ac_bot", "b_buy_bot", "b_sell_bot", "d_buy_bot", "d_sell_bot"):
                cur.execute(
                    """
                    INSERT IGNORE INTO bot_controls (bot_name, enabled)
                    VALUES (%s, 1)
                    """,
                    (bot_name,),
                )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS account_equity_snapshots (
                  id BIGINT AUTO_INCREMENT PRIMARY KEY,
                  equity DECIMAL(18,2) DEFAULT 0,
                  buying_power DECIMAL(18,2) DEFAULT 0,
                  cash DECIMAL(18,2) DEFAULT 0,
                  portfolio_value DECIMAL(18,2) DEFAULT 0,
                  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  INDEX idx_created_at (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )


def ensure_schema() -> None:
    """启动时统一执行所有 V1 表结构检查。"""
    ensure_stock_operations_columns()
    if settings().enable_position_holdings:
        ensure_position_holdings_table()
    ensure_control_state_tables()
