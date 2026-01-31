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
