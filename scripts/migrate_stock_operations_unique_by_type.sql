-- stock_operations 从 “每个 stock_code 只能一条” 迁移到
-- “每个 stock_code + stock_type 一条”。
--
-- 目的：
-- - 允许 QQQ/N 作为大盘风控开关。
-- - 同时允许 QQQ/C 作为策略 C 的期权交易队列行。
-- - 不再互相覆盖。
--
-- 执行前建议先备份：
--   CREATE TABLE stock_operations_backup_20260503 AS SELECT * FROM stock_operations;
--
-- 执行后验证：
--   SHOW INDEX FROM stock_operations;
--   SELECT stock_code, stock_type, entry_open, can_buy, can_sell
--   FROM stock_operations
--   WHERE stock_code='QQQ'
--   ORDER BY stock_type;

ALTER TABLE stock_operations
    DROP INDEX uk_stock_code,
    ADD UNIQUE KEY uk_stock_code_type (stock_code, stock_type);

-- 确保 QQQ/N 风控开关行存在。
-- 如果之前被 QQQ/C 覆盖，先修回 QQQ/N；迁移完成后，QQQ/C 可以单独插入。
INSERT INTO stock_operations (
    stock_code, stock_type, is_bought, can_buy, can_sell, entry_open,
    last_order_side, last_order_id, last_order_time, last_order_intent,
    created_at, updated_at
)
VALUES (
    'QQQ', 'N', 0, 0, 0, 0,
    NULL, NULL, NULL, 'N:GATE control row',
    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
)
ON DUPLICATE KEY UPDATE
    is_bought=0,
    can_buy=0,
    can_sell=0,
    last_order_side=NULL,
    last_order_id=NULL,
    last_order_time=NULL,
    last_order_intent='N:GATE control row',
    updated_at=CURRENT_TIMESTAMP;
