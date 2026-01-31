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
