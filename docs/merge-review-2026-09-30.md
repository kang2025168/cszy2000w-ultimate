# 2026-09-30 合并复核

## 合并来源

- 服务器与本地基线均为 `cab460d`，包含 Min 的性能优化、手动排除符号及 D 卖出腿 journal 修复。
- 保留 `99ef30d` 的安全修正：仅展示设置缓存 60 秒，交易、风控、账户和状态设置即时读取，不能恢复成全部设置缓存。
- 服务器原来未提交的慢日志配置与备份脚本已记录在 `d083c37`。其后补强备份失败检测和原子替换。
- 同时合入本地 B/D 风控改动，详见 `b-d-risk-rules.md`。迁移版本提升至5，保证现有版本4数据库也会新增原始买入时间字段。

## 实测与限制

- MySQL `innodb_buffer_pool_size=536870912`、`slow_query_log=ON`、`long_query_time=2`。
- 本次单次容器采样 MySQL CPU 4.53%，不足以验证开盘负载下的持续效果。
- stock_prices_pool.date、d_candidate_pool.signal_date、stock_price_category_snapshots.snapshot_date、strategy_b_levels.pressure_date 均为 DATE，相关函数移除保留原日期语义。
- stock_prices_pool.symbol 为 utf8mb4_0900_ai_ci；当前查询无须在过滤列套 UPPER。
- 券商资产查询中 HOUS、RAAQ、RAAQW 为 INACTIVE 且不可交易；EXPI 返回 APIError，未能据此确认退市。现有人工排除范围予以保留，空行情/接口错误不等于退市证明。
- 9/29 现有备份通过 gzip 完整性检查；这不是完整的还原演练。

## 运维修正

- 原脚本缺少 pipefail，导出失败仍可能由 gzip 生成非空文件并误报成功。
- 新脚本要求导出、压缩、gzip 校验及 mysqldump 完成标记全部通过，然后才原子替换当日文件并轮转。
- 密码由运行中容器内部环境读取，不作为宿主命令行参数。备份目录 0700，新文件 0600。
- backups/、agent_threads/ 保持服务器私有并已加入忽略规则。
- 服务器时区 America/Los_Angeles；已核对每日23:00备份以及修复后的价格/B候选/解锁/价位任务。现有 crontab 保持不变。
- 原应用日志轮转保持不变。MySQL 慢日志实际位于 data/mysql/*-slow.log，系统 mysql-server 轮转未覆盖；增加 ops/logrotate/cszy-mysql-slow（部署至 /etc/logrotate.d/，无需重启数据库）。

## B sizing 待定项（本次没有实施）

单笔不超过池目标25%的方向能减轻集中持仓，但不能只修改一行：

1. 原 remainder 最低金额为固定阈值，可能使约439美元的单笔开两笔后无法继续开仓；必须与正常单笔、剩余半笔及最小交易额共同定义。
2. 动态计划的 max_positions 由剩余额度推算，未直接限制在配置的 B_MAX_ACTIVE_POSITIONS；不能声称已有固定四仓保证。
3. 当前 available 使用 target-used 而非 allocation.available；需核对挂单占用与原风险门。
4. 获取资金失败会回退静态金额，25%约束必须同时处理静态/失败路径，避免绕过。
5. 新仓额度调整不会自动把现有 SWKS 减到25%；存量由卖出风控处理。

这里不把尚未定案的25%仓位政策混入已确认的止损规则。

## 合并验证

- 离线单元测试180项通过，隔离 MySQL 集成测试15项通过。
- 包含旧版本4迁移、账户/策略隔离、部分成交、失败回滚、重启恢复、止损门槛与备份失败原子性检查。
- Dashboard JavaScript、备份 shell 语法和 git diff 格式检查通过。
