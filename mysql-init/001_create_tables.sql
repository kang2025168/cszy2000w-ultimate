CREATE DATABASE IF NOT EXISTS cszy2000 DEFAULT CHARSET utf8mb4 COLLATE utf8mb4_general_ci;
USE cszy2000;

CREATE TABLE IF NOT EXISTS stock_prices_pool (
  symbol VARCHAR(16) NOT NULL,
  date DATETIME NOT NULL,
  open DOUBLE NULL,
  high DOUBLE NULL,
  low  DOUBLE NULL,
  close DOUBLE NULL,
  volume BIGINT NULL,
  PRIMARY KEY (symbol, date),
  KEY idx_date (date),
  KEY idx_symbol (symbol)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS stock_operations (
  stock_code VARCHAR(16) NOT NULL,
  close_price DOUBLE NULL,
  trigger_price DOUBLE NULL,
  cost_price DOUBLE NULL,
  stop_loss_price DOUBLE NULL,
  take_profit_price DOUBLE NULL,
  weight DOUBLE NULL,
  stock_type VARCHAR(2) NOT NULL DEFAULT 'A',
  is_bought TINYINT NOT NULL DEFAULT 0,
  qty INT NOT NULL DEFAULT 0,
  can_buy TINYINT NOT NULL DEFAULT 0,
  can_sell TINYINT NOT NULL DEFAULT 0,
  last_order_intent VARCHAR(80) NULL,
  last_order_side VARCHAR(8) NULL,
  last_order_id VARCHAR(64) NULL,
  last_order_time DATETIME NULL,
  created_at DATETIME NULL,
  updated_at DATETIME NULL,
  PRIMARY KEY (stock_code),
  KEY idx_stock_type (stock_type),
  KEY idx_updated_at (updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS stock_price_category_snapshots (
  snapshot_date DATE NOT NULL,
  category_group VARCHAR(32) NOT NULL,
  category_group_label VARCHAR(64) NOT NULL,
  category_key VARCHAR(64) NOT NULL,
  category_label VARCHAR(64) NOT NULL,
  category_order INT NOT NULL,
  symbol VARCHAR(16) NOT NULL,
  open DOUBLE NULL,
  high DOUBLE NULL,
  low DOUBLE NULL,
  close DOUBLE NULL,
  volume BIGINT NULL,
  change_pct DOUBLE NULL,
  up_streak INT NOT NULL DEFAULT 0,
  down_streak INT NOT NULL DEFAULT 0,
  up_days_2 INT NULL,
  up_days_3 INT NULL,
  up_days_4 INT NULL,
  up_days_5 INT NULL,
  down_days_2 INT NULL,
  down_days_3 INT NULL,
  down_days_4 INT NULL,
  down_days_5 INT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (snapshot_date, category_key, symbol),
  KEY idx_snapshot_order (snapshot_date, category_order),
  KEY idx_symbol_date (symbol, snapshot_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
