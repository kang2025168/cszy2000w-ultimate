from __future__ import annotations

"""数据库自动迁移：补齐旧表字段，并创建持仓展示表。"""

from .config import settings
from .db import db_conn


STOCK_OPERATION_COLUMNS = {
    "current_price": "DECIMAL(18,6) NULL",
    "intraday_volume": "BIGINT NULL",
    "strategy_group": "VARCHAR(8) NULL",
    "capital_pool": "VARCHAR(16) NULL",
    "margin_used": "TINYINT DEFAULT 0",
    "rebalance_date": "DATE NULL",
    "last_capital_check_at": "DATETIME NULL",
    "buy_signal": "TINYINT DEFAULT 0",
    "sell_signal": "TINYINT DEFAULT 0",
    "signal_action": "VARCHAR(32) NULL",
    "signal_price": "DECIMAL(18,6) NULL",
    "signal_prev_close": "DECIMAL(18,6) NULL",
    "signal_day_open": "DECIMAL(18,6) NULL",
    "signal_day_high": "DECIMAL(18,6) NULL",
    "signal_day_low": "DECIMAL(18,6) NULL",
    "signal_volume": "BIGINT NULL",
    "signal_source": "VARCHAR(64) NULL",
    "signal_reason": "VARCHAR(512) NULL",
    "signal_at": "DATETIME NULL",
    "ac_t_enabled": "TINYINT DEFAULT 1",
    "ac_t_type": "VARCHAR(10) NULL",
    "ac_t_state": "VARCHAR(40) DEFAULT 'IDLE'",
    "ac_t_base_price": "DECIMAL(12,4) NULL",
    "ac_t_base_date": "DATE NULL",
    "ac_t_open_price": "DECIMAL(12,4) NULL",
    "ac_t_open_date": "DATE NULL",
    "ac_t_open_mode": "VARCHAR(20) NULL",
    "ac_t_trade_high_price": "DECIMAL(12,4) NULL",
    "ac_t_trade_low_price": "DECIMAL(12,4) NULL",
    "ac_t_extreme_confirmed": "TINYINT DEFAULT 0",
    "ac_t_core_qty": "INT DEFAULT 0",
    "ac_t_qty": "INT DEFAULT 0",
    "ac_t_buy_price": "DECIMAL(12,4) NULL",
    "ac_t_sell_price": "DECIMAL(12,4) NULL",
    "ac_t_high_price": "DECIMAL(12,4) NULL",
    "ac_t_low_price": "DECIMAL(12,4) NULL",
    "ac_t_entry_time": "DATETIME NULL",
    "ac_t_last_action_date": "DATE NULL",
    "ac_t_last_action_side": "VARCHAR(20) NULL",
    "ac_t_last_up_date": "DATE NULL",
    "ac_t_last_down_date": "DATE NULL",
    "ac_t_temporarily_out": "TINYINT DEFAULT 0",
    "ac_t_force_recover_deadline": "DATETIME NULL",
}

RISK_STATE_COLUMNS = {
    "block_a_buy": "TINYINT DEFAULT 0",
    "block_c_buy": "TINYINT DEFAULT 0",
    "market_trend": "VARCHAR(16) DEFAULT '横盘'",
    "market_reason": "VARCHAR(512) DEFAULT ''",
    "qqq_change_pct": "DECIMAL(10,4) DEFAULT 0",
    "vix": "DECIMAL(10,4) DEFAULT 18",
    "risk_preference": "VARCHAR(16) DEFAULT '中性'",
    "allocation_mode": "VARCHAR(16) DEFAULT '动态分仓'",
    "recommended_exposure": "DECIMAL(10,4) DEFAULT 0.6",
    "recommended_weights": "JSON NULL",
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


def _index_exists(conn, table: str, index: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS n
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = %s
              AND INDEX_NAME = %s
            """,
            (table, index),
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


def _column_data_type(conn, table: str, column: str) -> str:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DATA_TYPE
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = %s
              AND COLUMN_NAME = %s
            """,
            (table, column),
        )
        row = cur.fetchone() or {}
        return str(row.get("DATA_TYPE") or "").lower()


def ensure_stock_operations_columns() -> None:
    """检查旧交易控制表，缺少 V1 需要的字段就自动添加。"""
    s = settings()
    with db_conn(s) as conn:
        for column, ddl in STOCK_OPERATION_COLUMNS.items():
            if not _column_exists(conn, s.ops_table, column):
                with conn.cursor() as cur:
                    cur.execute(f"ALTER TABLE `{s.ops_table}` ADD COLUMN `{column}` {ddl}")
                print(f"[SCHEMA] added {s.ops_table}.{column}", flush=True)
        stock_code_len = _varchar_length(conn, s.ops_table, "stock_code")
        if stock_code_len is not None and stock_code_len < 64:
            with conn.cursor() as cur:
                cur.execute(f"ALTER TABLE `{s.ops_table}` MODIFY COLUMN stock_code VARCHAR(64) NOT NULL")
            print(f"[SCHEMA] upgraded {s.ops_table}.stock_code to VARCHAR(64)", flush=True)
        for quantity_column in ("qty", "ac_t_core_qty", "ac_t_qty"):
            data_type = _column_data_type(conn, s.ops_table, quantity_column)
            if data_type and data_type != "decimal":
                with conn.cursor() as cur:
                    cur.execute(
                        f"ALTER TABLE `{s.ops_table}` "
                        f"MODIFY COLUMN `{quantity_column}` DECIMAL(18,6) DEFAULT 0"
                    )
                print(
                    f"[SCHEMA] upgraded {s.ops_table}.{quantity_column} to DECIMAL(18,6)",
                    flush=True,
                )
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
                  initial_entry_price DECIMAL(18,6),
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
            if not _column_exists(conn, "position_holdings", "initial_entry_price"):
                cur.execute("ALTER TABLE position_holdings ADD COLUMN initial_entry_price DECIMAL(18,6) NULL AFTER qty")
                cur.execute(
                    """
                    UPDATE position_holdings
                    SET initial_entry_price=avg_entry_price
                    WHERE initial_entry_price IS NULL
                      AND avg_entry_price IS NOT NULL
                    """
                )
                print("[SCHEMA] added position_holdings.initial_entry_price", flush=True)


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
	                  block_a_buy TINYINT DEFAULT 0,
	                  block_b_buy TINYINT DEFAULT 0,
	                  block_c_buy TINYINT DEFAULT 0,
	                  block_d_buy TINYINT DEFAULT 0,
	                  suggest_capital_mode VARCHAR(16),
	                  reason VARCHAR(128),
	                  market_trend VARCHAR(16) DEFAULT '横盘',
	                  market_reason VARCHAR(512) DEFAULT '',
	                  qqq_change_pct DECIMAL(10,4) DEFAULT 0,
	                  vix DECIMAL(10,4) DEFAULT 18,
	                  risk_preference VARCHAR(16) DEFAULT '中性',
	                  allocation_mode VARCHAR(16) DEFAULT '动态分仓',
	                  recommended_exposure DECIMAL(10,4) DEFAULT 0.6,
	                  recommended_weights JSON NULL,
                  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  INDEX idx_updated_at (updated_at)
	                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
	                """
	            )
            for column, ddl in RISK_STATE_COLUMNS.items():
                if not _column_exists(conn, "risk_state", column):
                    cur.execute(f"ALTER TABLE risk_state ADD COLUMN {column} {ddl}")
                    print(f"[SCHEMA] added risk_state.{column}", flush=True)
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS app_settings (
                  setting_key VARCHAR(128) PRIMARY KEY,
                  setting_value MEDIUMTEXT NOT NULL,
                  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
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
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bot_lifecycle_events (
                  id BIGINT AUTO_INCREMENT PRIMARY KEY,
                  bot_name VARCHAR(64) NOT NULL,
                  action VARCHAR(16) NOT NULL,
                  status VARCHAR(32) NOT NULL,
                  pid INT NULL,
                  message VARCHAR(255),
                  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  INDEX idx_created_at (created_at),
                  INDEX idx_bot_created (bot_name, created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            default_controls = {
                "dashboard_bot": 1,
                "risk_bot": 1,
                "rebalance_bot": 0,
                "ac_bot": 1,
                "b_buy_bot": 1,
                "b_sell_bot": 1,
                "f_buy_bot": 0,
                "f_sell_bot": 0,
                "d_buy_bot": 0,
                "d_sell_bot": 0,
                "d_grid_bot": 0,
                "q_sell_bot": 1,
            }
            for bot_name, enabled in default_controls.items():
                cur.execute(
                    """
                    INSERT IGNORE INTO bot_controls (bot_name, enabled)
                    VALUES (%s, %s)
                    """,
                    (bot_name, enabled),
                )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS account_equity_snapshots (
                  id BIGINT AUTO_INCREMENT PRIMARY KEY,
                  broker_profile VARCHAR(32) NOT NULL DEFAULT 'trading',
                  equity DECIMAL(18,2) DEFAULT 0,
                  buying_power DECIMAL(18,2) DEFAULT 0,
                  cash DECIMAL(18,2) DEFAULT 0,
                  portfolio_value DECIMAL(18,2) DEFAULT 0,
                  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  INDEX idx_created_at (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            if not _column_exists(conn, "account_equity_snapshots", "broker_profile"):
                cur.execute(
                    """
                    ALTER TABLE account_equity_snapshots
                    ADD COLUMN broker_profile VARCHAR(32) NOT NULL DEFAULT 'legacy' AFTER id
                    """
                )
            if not _index_exists(conn, "account_equity_snapshots", "idx_equity_profile_created"):
                cur.execute(
                    """
                    CREATE INDEX idx_equity_profile_created
                    ON account_equity_snapshots (broker_profile, created_at)
                    """
                )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS exposure_state (
                  id BIGINT AUTO_INCREMENT PRIMARY KEY,
                  mode VARCHAR(16) NOT NULL DEFAULT 'SUGGEST',
                  risk_mode VARCHAR(16) NULL,
                  market_trend VARCHAR(16) NULL,
                  vix DECIMAL(10,4) DEFAULT 0,
                  equity DECIMAL(18,2) DEFAULT 0,
                  current_market_value DECIMAL(18,2) DEFAULT 0,
                  current_exposure_pct DECIMAL(10,6) DEFAULT 0,
                  target_market_value DECIMAL(18,2) DEFAULT 0,
                  target_exposure_pct DECIMAL(10,6) DEFAULT 0,
                  exposure_gap_value DECIMAL(18,2) DEFAULT 0,
                  exposure_gap_pct DECIMAL(10,6) DEFAULT 0,
                  scale_ratio DECIMAL(10,6) DEFAULT 1,
                  action VARCHAR(16) NOT NULL DEFAULT 'HOLD',
                  reason VARCHAR(255) NULL,
                  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  INDEX idx_created_at (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS rebalance_actions (
                  id BIGINT AUTO_INCREMENT PRIMARY KEY,
                  round_id VARCHAR(64) NOT NULL,
                  symbol VARCHAR(64) NOT NULL,
                  strategy_group VARCHAR(8) NOT NULL,
                  side VARCHAR(8) NOT NULL,
                  current_value DECIMAL(18,2) DEFAULT 0,
                  target_value DECIMAL(18,2) DEFAULT 0,
                  delta_value DECIMAL(18,2) DEFAULT 0,
                  qty DECIMAL(18,6) DEFAULT 0,
                  price DECIMAL(18,6) DEFAULT 0,
                  status VARCHAR(32) NOT NULL DEFAULT 'planned',
                  reason VARCHAR(255) NULL,
                  order_id VARCHAR(128) NULL,
                  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  executed_at DATETIME NULL,
                  INDEX idx_round_id (round_id),
                  INDEX idx_symbol_status (symbol, status),
                  INDEX idx_created_at (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS manual_trade_records (
                  id BIGINT AUTO_INCREMENT PRIMARY KEY,
                  event_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  symbol VARCHAR(32) NOT NULL,
                  side VARCHAR(12) NOT NULL,
                  strategy_group VARCHAR(8) NOT NULL,
                  qty DECIMAL(18,6) NOT NULL DEFAULT 0,
                  filled_qty DECIMAL(18,6) NOT NULL DEFAULT 0,
                  price DECIMAL(18,6) NOT NULL DEFAULT 0,
                  filled_avg_price DECIMAL(18,6) NOT NULL DEFAULT 0,
                  order_type VARCHAR(16) NOT NULL DEFAULT 'limit',
                  status VARCHAR(32) NOT NULL DEFAULT 'submitted',
                  note VARCHAR(512) NULL,
                  order_id VARCHAR(128) NOT NULL,
                  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  UNIQUE KEY uk_manual_trade_order_id (order_id),
                  INDEX idx_manual_trade_event_time (event_time),
                  INDEX idx_manual_trade_symbol (symbol)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS strategy_c_core_buys (
                  id BIGINT AUTO_INCREMENT PRIMARY KEY,
                  trade_date DATE NOT NULL,
                  symbol VARCHAR(32) NOT NULL,
                  tier INT NOT NULL DEFAULT 3,
                  target_weight DECIMAL(10,6) NOT NULL DEFAULT 0,
                  planned_notional DECIMAL(18,2) NOT NULL DEFAULT 0,
                  limit_price DECIMAL(18,6) NOT NULL DEFAULT 0,
                  filled_qty DECIMAL(18,6) NOT NULL DEFAULT 0,
                  filled_avg_price DECIMAL(18,6) NOT NULL DEFAULT 0,
                  status VARCHAR(32) NOT NULL DEFAULT 'planned',
                  reason VARCHAR(255) NULL,
                  order_id VARCHAR(128) NULL,
                  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  UNIQUE KEY uk_c_core_order_id (order_id),
                  INDEX idx_c_core_trade_date (trade_date),
                  INDEX idx_c_core_symbol_date (symbol, trade_date),
                  INDEX idx_c_core_status (status)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS d_grid_symbols (
                  symbol VARCHAR(32) PRIMARY KEY,
                  enabled TINYINT NOT NULL DEFAULT 0,
                  lot_notional DECIMAL(18,2) NOT NULL DEFAULT 250,
                  entry_offset DECIMAL(18,4) NOT NULL DEFAULT 0.03,
                  profit_offset DECIMAL(18,4) NOT NULL DEFAULT 0.06,
                  max_spread DECIMAL(18,4) NOT NULL DEFAULT 0.05,
                  sort_order INT NOT NULL DEFAULT 0,
                  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS d_candidate_pool (
                  symbol VARCHAR(32) PRIMARY KEY,
                  signal_date DATE NOT NULL,
                  signal_close DECIMAL(18,6) NOT NULL DEFAULT 0,
                  signal_gain_pct DECIMAL(12,6) NOT NULL DEFAULT 0,
                  signal_volume BIGINT NOT NULL DEFAULT 0,
                  signal_dollar_volume DECIMAL(20,2) NOT NULL DEFAULT 0,
                  avg_range_pct DECIMAL(12,6) NOT NULL DEFAULT 0,
                  base_score DECIMAL(12,4) NOT NULL DEFAULT 0,
                  enabled TINYINT NOT NULL DEFAULT 1,
                  selected_count INT NOT NULL DEFAULT 0,
                  last_selected_at DATETIME NULL,
                  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  INDEX idx_d_candidate_date (signal_date),
                  INDEX idx_d_candidate_enabled (enabled, signal_date)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS d_grid_cycles (
                  symbol VARCHAR(32) PRIMARY KEY,
                  state VARCHAR(24) NOT NULL DEFAULT 'IDLE',
                  cycle_no BIGINT NOT NULL DEFAULT 0,
                  anchor_price DECIMAL(18,6) DEFAULT 0,
                  buy_limit DECIMAL(18,6) DEFAULT 0,
                  buy_order_id VARCHAR(128),
                  buy_qty DECIMAL(18,6) DEFAULT 0,
                  buy_filled_qty DECIMAL(18,6) DEFAULT 0,
                  buy_filled_price DECIMAL(18,6) DEFAULT 0,
                  sell_limit DECIMAL(18,6) DEFAULT 0,
                  sell_order_id VARCHAR(128),
                  sell_filled_price DECIMAL(18,6) DEFAULT 0,
                  realized_pnl DECIMAL(18,2) DEFAULT 0,
                  cooldown_until DATETIME,
                  last_error VARCHAR(512),
                  state_changed_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  INDEX idx_d_grid_state (state)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS d_grid_events (
                  id BIGINT AUTO_INCREMENT PRIMARY KEY,
                  symbol VARCHAR(32) NOT NULL,
                  cycle_no BIGINT NOT NULL DEFAULT 0,
                  event_type VARCHAR(32) NOT NULL,
                  state VARCHAR(24) NOT NULL,
                  order_id VARCHAR(128),
                  qty DECIMAL(18,6) DEFAULT 0,
                  price DECIMAL(18,6) DEFAULT 0,
                  message VARCHAR(512),
                  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  INDEX idx_d_grid_event_symbol (symbol, created_at),
                  INDEX idx_d_grid_event_created (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS ac_t_cycle_results (
                  id BIGINT AUTO_INCREMENT PRIMARY KEY,
                  strategy_group VARCHAR(8) NOT NULL,
                  symbol VARCHAR(32) NOT NULL,
                  direction VARCHAR(32) NOT NULL,
                  entry_side VARCHAR(8) NOT NULL,
                  exit_side VARCHAR(8) NOT NULL,
                  qty DECIMAL(18,6) NOT NULL DEFAULT 0,
                  entry_price DECIMAL(18,6) NOT NULL DEFAULT 0,
                  exit_price DECIMAL(18,6) NOT NULL DEFAULT 0,
                  realized_pnl DECIMAL(18,6) NOT NULL DEFAULT 0,
                  return_pct DECIMAL(12,8) NOT NULL DEFAULT 0,
                  cost_effect VARCHAR(16) NOT NULL,
                  exit_reason VARCHAR(64),
                  exit_order_id VARCHAR(128),
                  started_at DATETIME NULL,
                  completed_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  UNIQUE KEY uq_ac_t_exit_order (exit_order_id),
                  INDEX idx_ac_t_completed (completed_at),
                  INDEX idx_ac_t_group_symbol (strategy_group, symbol, completed_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )


def ensure_strategy_lot_identity(conn) -> None:
    """Permit the same symbol in isolated strategy lots without deleting data."""
    table = settings().ops_table
    with conn.cursor() as cur:
        cur.execute("SELECT INDEX_NAME,GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS cols FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s AND NON_UNIQUE=0 GROUP BY INDEX_NAME", (table,))
        indexes = list(cur.fetchall())
        if not any(row["cols"] in {"stock_code,stock_type", "stock_type,stock_code"} for row in indexes):
            cur.execute(f"ALTER TABLE `{table}` ADD UNIQUE KEY uq_strategy_lot (stock_code,stock_type)")
        for row in indexes:
            if row["cols"] == "stock_code":
                name = row["INDEX_NAME"]
                clause = "DROP PRIMARY KEY" if name == "PRIMARY" else f"DROP INDEX `{name}`"
                cur.execute(f"ALTER TABLE `{table}` {clause}")


SCHEMA_VERSION = 4
_SCHEMA_READY = False
from threading import Lock
_SCHEMA_LOCK = Lock()


def ensure_schema() -> None:
    """Apply versioned migrations once, serialized across all worker processes."""
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return
    with _SCHEMA_LOCK:
        if _SCHEMA_READY:
            return
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT GET_LOCK('cszy:schema_migration', 60) AS acquired")
                if int((cur.fetchone() or {}).get("acquired") or 0) != 1:
                    raise RuntimeError("Schema migration lock unavailable")
                try:
                    cur.execute("CREATE TABLE IF NOT EXISTS schema_migrations (version INT PRIMARY KEY, applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP)")
                    cur.execute("SELECT version FROM schema_migrations WHERE version=%s", (SCHEMA_VERSION,))
                    if not cur.fetchone():
                        ensure_stock_operations_columns()
                        ensure_position_holdings_table()
                        ensure_control_state_tables()
                        if _column_data_type(conn, 'app_settings', 'setting_value') != 'mediumtext':
                            cur.execute('ALTER TABLE app_settings MODIFY COLUMN setting_value MEDIUMTEXT NOT NULL')
                        from .order_journal import ensure_table
                        ensure_table(conn)
                        ensure_strategy_lot_identity(conn)
                        if not _column_exists(conn, "d_grid_cycles", "pending_client_order_id"):
                            cur.execute("ALTER TABLE d_grid_cycles ADD COLUMN pending_client_order_id VARCHAR(64) NULL")
                        cur.execute("INSERT INTO schema_migrations (version) VALUES (%s)", (SCHEMA_VERSION,))
                        conn.commit()
                finally:
                    cur.execute("SELECT RELEASE_LOCK('cszy:schema_migration')")
        _SCHEMA_READY = True


if __name__ == "__main__":
    ensure_schema()
