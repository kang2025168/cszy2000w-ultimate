-- 001_init_tables.sql
-- 进入默认库（docker 用 MYSQL_DATABASE 创建的就是 DB_NAME）
-- 如果你想更保险也可以写：USE cszy2000; 但这里用环境变量创建的库名即可

CREATE TABLE IF NOT EXISTS stock_prices_pool (
  symbol VARCHAR(10) NOT NULL,
  date   DATE NOT NULL,
  open   DOUBLE DEFAULT NULL,
  high   DOUBLE DEFAULT NULL,
  low    DOUBLE DEFAULT NULL,
  close  DOUBLE DEFAULT NULL,
  volume BIGINT DEFAULT NULL,
  PRIMARY KEY (symbol, date),
  KEY idx_date (date),
  KEY idx_symbol (symbol)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
