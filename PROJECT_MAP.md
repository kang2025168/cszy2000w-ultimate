# Project Map

## 运行拓扑

- `docker-compose.yml`：MySQL、B/F 四个独立买卖容器、Ultimate 网页与机器人监管、旧手机看板、行情分类更新。
- 启动完整拓扑：`docker compose --profile split-bots --profile ultimate up -d --build`。
- `scripts/run.sh`：入口分发；支持容器与项目目录运行。
- `app/bots/runtime_core.py` / `split_core.py`：B/F 调度、时间窗口、开关与心跳。
- `ultimate_v1/bot_supervisor.py`：其他机器人子进程监管；外部容器机器人不重复拉起。

## 交易与状态

- `app/b_config.py`：B 策略环境参数；`app/strategy_b.py`：B 策略行为。
- `ultimate_v1/order_fills.py`：基于订单回报确认成交，不使用总持仓推断成交。
- `ultimate_v1/account_config.py` / `broker_transport.py` / `alpaca_gateway.py`：账户隔离、身份校验、HTTP 连接与券商访问。
- `ultimate_v1/order_journal.py`：手动与 D 网格订单意图、幂等提交、资金预占。
- `ultimate_v1/manual_execution.py`：手动与强平订单执行服务。
- `ultimate_v1/manual_ledger.py`：手动订单累计成交对账、事务入账与后台恢复。
- `ultimate_v1/capital_manager.py` / `risk_controller.py` / `trading_gate.py`：额度、风险和新开仓检查。
- `ultimate_v1/d_grid.py`：D 网格状态机与提交恢复。
- `ultimate_v1/sync_positions.py` / `position_holdings.py`：券商持仓与策略持仓同步。
- `ultimate_v1/schema.py`：版本化迁移；`db.py`：有界连接池与事务。

## 网页与运维

- `ultimate_v1/web_app.py`：HTTP/API 与业务适配。
- `ultimate_v1/templates/`：独立网页模板。
- `ultimate_v1/web_auth.py`：有期限会话、密码配置与限速。
- `ultimate_v1/dashboard_cache.py`：后台展示快照，与执行检查分离。
- `ultimate_v1/metrics.py`：有界请求耗时与计数。
- `tests/`：离线回归测试。
- `scripts/test_execution_mysql.py`：隔离 MySQL 事务、幂等和恢复测试。
- `.github/workflows/tests.yml`：Python 3.12 + MySQL CI。

部署、迁移、备份与故障处理见 [运行手册](docs/OPERATIONS.md)。审查基线见 PROJECT_AUDIT_2026-09-22.md。
