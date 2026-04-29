# Project Map

这份文档只记录当前项目结构和运行链路，方便以后改代码前先确认影响范围。它不改变任何交易逻辑。

## 当前结论

这个项目是一个 Python 交易机器人，使用 Docker Compose 启动两个主要服务：

- `mysql`: MySQL 8.0，保存行情池和交易状态。
- `tradebot`: Python 程序，读取 MySQL 中的股票状态，调用策略函数，并通过 Alpaca 下单。

当前主运行链路是：

```text
docker compose up -d --build
  -> tradebot service
  -> ./scripts/run.sh strategy_a
  -> python -u app/trade_bot_main.py
  -> import strategy_a / strategy_b / strategy_c / strategy_d / strategy_e
  -> main_loop()
  -> one_round()
  -> load_rows()
  -> dispatch_one()
  -> strategy_B_buy() / strategy_B_sell()
```

注意：虽然 Docker 命令里写的是 `strategy_a`，但实际主程序当前只在 `dispatch_one()` 里对 `stock_type == "B"` 调用买卖策略。A/C/D/E 目前被导入，但主分发里没有实际执行它们。

## 关键文件

### 启动和部署

- `docker-compose.yml`
  - 定义 `mysql` 和 `tradebot`。
  - `tradebot` 使用 `.env`，并把 `ALPACA_MODE` 映射为 `TRADE_ENV`。
  - 默认命令是 `./scripts/run.sh strategy_a`。

- `Dockerfile`
  - 基于 `python:3.12-slim`。
  - 安装 `requirements.txt`。
  - 默认 CMD 是 healthcheck，但在 compose 中会被覆盖。

- `scripts/run.sh`
  - 先执行 `app/healthcheck.py`。
  - `main` 和 `strategy_a` 都会运行 `app/trade_bot_main.py`。
  - 还支持 `getdata_full`、`unlock_can_sell`、`healthcheck`。

### 主循环

- `app/trade_bot_main.py`
  - 当前主入口。
  - 读取 `TRADE_ENV` / `ALPACA_MODE`，只允许 `paper` 或 `live`。
  - 根据环境把 paper/live key 注入到通用变量：
    - `APCA_API_KEY_ID`
    - `APCA_API_SECRET_KEY`
    - `ALPACA_KEY`
    - `ALPACA_SECRET`
  - 控制交易时间：美西时间 `06:40` 到 `13:00`。
  - 控制买入开关：
    - Alpaca buying power 是否高于 `MIN_BUYING_POWER`。
    - `stock_operations` 中 `QQQ` / `stock_type='N'` 的 `entry_open` 是否为 `1`。
  - 先扫描可卖股票，再扫描可买股票。

### 策略

- `app/strategy_b.py`
  - 当前主循环实际调用的 B 策略实现。
  - 主要函数：
    - `strategy_B_buy(code)`
    - `strategy_B_sell(code)`

- `app/strategy_a.py`
  - A 策略买卖逻辑存在：
    - `strategy_A_buy(stock_code)`
    - `strategy_A_sell(stock_code)`
  - 当前主循环没有分发到 A 策略。

- `app/strategy_c.py`
  - 看起来偏向筛选/生成候选，而不是当前主循环直接交易。

- `app/strategy_d.py` / `app/strategy_e.py`
  - 目前只有简单打印函数。

### 数据和表

- `mysql-init/001_create_tables.sql`
  - 创建 `stock_prices_pool`。
  - 创建 `stock_operations`。

- `stock_prices_pool`
  - 历史行情池。
  - 主要字段：`symbol`、`date`、`open`、`high`、`low`、`close`、`volume`。

- `stock_operations`
  - 当前交易状态表。
  - 主要字段：
    - `stock_code`
    - `stock_type`
    - `is_bought`
    - `qty`
    - `can_buy`
    - `can_sell`
    - `cost_price`
    - `close_price`
    - `trigger_price`
    - `stop_loss_price`
    - `take_profit_price`
    - `last_order_intent`
    - `last_order_side`
    - `last_order_id`
    - `last_order_time`

### 数据同步和辅助脚本

- `app/getdata_alpaca.py`
  - 从 Alpaca 拉行情，写入 `stock_prices_pool`。

- `scripts/sync_positions_to_ops.py`
  - 从 Alpaca 当前持仓同步到 `stock_operations`。

- `scripts/sync_positions_simple.py`
  - 简化版持仓同步脚本。

- `app/unlock_can_sell.py`
  - 解锁可卖状态相关脚本。

- `app/risk_gate_qqq.py`
  - 根据 QQQ 风险状态更新 `entry_open`，作为主循环的大盘买入开关。

- `scripts/backtest_strategy_b_pool.py`
  - B 策略回测。

- `scripts/validate_strategy_b_from_date.py`
  - 从指定日期验证 B 策略。

- `scripts/analyze_access_key_trades.py`
  - 分析 Alpaca access key 对应交易记录。

## 配置变量

项目里主要依赖这些环境变量：

- `ALPACA_MODE`: `paper` 或 `live`。
- `TRADE_ENV`: 主程序会兼容读取；compose 中由 `ALPACA_MODE` 映射。
- `PAPER_APCA_API_KEY_ID`
- `PAPER_APCA_API_SECRET_KEY`
- `PAPER_ALPACA_BASE_URL`
- `LIVE_APCA_API_KEY_ID`
- `LIVE_APCA_API_SECRET_KEY`
- `LIVE_ALPACA_BASE_URL`
- `DB_HOST`
- `DB_PORT`
- `DB_USER`
- `DB_PASS`
- `DB_NAME`
- `OPS_TABLE`
- `GETDATA_TABLE`
- `MIN_BUYING_POWER`
- `BUYPOWER_REFRESH_SECS`
- `SLEEP_BETWEEN_SYMBOLS`
- `SLEEP_BETWEEN_ROUNDS`
- `LOG_DIR`

`.env` 已经在 `.gitignore` 中，不应提交到 git。

## 运行命令

常用启动：

```bash
docker compose up -d --build
```

查看交易机器人日志：

```bash
docker compose logs -f tradebot
```

查看 MySQL 日志：

```bash
docker compose logs -f mysql
```

进入 MySQL：

```bash
docker compose exec mysql mysql -u tradebot -p"$MYSQL_ROOT_PASSWORD" cszy2000
```

只跑健康检查：

```bash
docker compose run --rm tradebot ./scripts/run.sh healthcheck
```

## 当前低风险观察

这些点只是观察，不代表必须马上改：

- `requirements.txt` 中 `pandas-datareader>=0.10.0` 重复了一次。
- 代码里存在多个旧入口或备份式入口，例如：
  - `app/mainbott.py`
  - `app/jiqireyuanban.py`
  - `app/trade22_bot_main.py`
  - `app/strategy_b-old.py`
  - `app/strategy_b_v2.py`
- 多个脚本里还保留数据库默认密码字符串。即使 `.env` 没提交，后续也建议逐步统一为“必须从环境变量读取”。
- `app/AAA-UI.py` 看起来默认偏向 live 配置，使用前应再次确认环境保护。
- 主入口文件中有大段旧逻辑注释，短期不影响运行，但长期会增加维护难度。
- `app/common/db.py` 和 `app/common/log.py` 目前为空文件，说明公共层还没真正收口。

## 建议的安全改动顺序

如果以后要整理，建议按下面顺序，每一步都单独验证：

1. 只改文档，不动运行逻辑。
2. 清理 `requirements.txt` 的重复依赖。
3. 增加一个只读检查脚本，打印当前环境、入口、DB 表连接状态，不下单。
4. 给 live 模式增加更明显的启动确认日志或保护开关。
5. 统一 DB 配置读取方式。
6. 标记旧入口，确认没有使用后再归档。
7. 最后才考虑改 `trade_bot_main.py`、`strategy_a.py`、`strategy_b.py` 的交易逻辑。

## 改代码前检查清单

在修改交易相关文件前，先确认：

- 当前是在 `paper` 还是 `live`。
- 是否有 MySQL 备份。
- `docker compose logs -f tradebot` 中是否已经稳定运行。
- 改动是否会影响：
  - 下单 key 选择。
  - `can_buy` / `can_sell`。
  - `is_bought`。
  - `qty`。
  - `last_order_intent`。
  - `entry_open` 大盘开关。
- 是否可以先在 paper 模式跑一轮。

## 本次检查结果

已执行轻量语法检查：

```bash
python -m compileall -q app scripts
```

结果：通过。

没有启动 Docker，没有连接 Alpaca，没有触发交易。
