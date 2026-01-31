USE cszy2000;

CREATE TABLE IF NOT EXISTS stock_operations (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  stock_code VARCHAR(16) NOT NULL,
  stock_type VARCHAR(8) NOT NULL DEFAULT 'A',

  weight DOUBLE NULL,
  trigger_price DOUBLE NULL,

  is_bought TINYINT NOT NULL DEFAULT 0,
  can_buy TINYINT NOT NULL DEFAULT 0,
  can_sell TINYINT NOT NULL DEFAULT 0,

  qty INT NULL,
  cost_price DOUBLE NULL,

  take_profit_price DOUBLE NULL,
  stop_loss_price DOUBLE NULL,
  close_price DOUBLE NULL,

  last_order_intent VARCHAR(80) NULL,
  last_order_side VARCHAR(8) NULL,
  last_order_id VARCHAR(64) NULL,
  last_order_time DATETIME NULL,

  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  PRIMARY KEY (id),
  UNIQUE KEY uk_stock_code (stock_code),
  KEY idx_type (stock_type),
  KEY idx_flags (stock_type, is_bought, can_buy, can_sell)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
