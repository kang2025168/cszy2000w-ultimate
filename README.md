## 运行方式（Docker Compose）

本项目通过 Docker Compose 运行，所有配置来自 `.env`。

### 关键入口

- 主交易服务：`buybot` / `sellbot` / `f_buybot` / `f_sellbot`
- 机器人运行核心：`app/bots/runtime_core.py`
- B 买入机器人：`app/bots/b_buy_bot.py`
- B 卖出机器人：`app/bots/b_sell_bot.py`
- F 买入机器人：`app/bots/f_buy_bot.py`
- F 卖出机器人：`app/bots/f_sell_bot.py`
- 机器人共享运行层：`app/bots/split_core.py`
- ABCD 买卖策略统一入口：`app/strategies/abcd_strategy.py`

机器人运行状态、日志、开关和排障命令详见：`ultimate_v1/README.md` 的“机器人运行查看和排障命令”。

### 买卖机器人拆分

每个策略的买入和卖出都可以单独运行：

```bash
./scripts/run.sh b_buy_bot
./scripts/run.sh b_sell_bot
./scripts/run.sh f_buy_bot
./scripts/run.sh f_sell_bot
```

Docker 里已经预留了 `split-bots` profile，默认不影响现有 `tradebot`：

```bash
docker compose --profile split-bots up -d --build buybot sellbot f_buybot f_sellbot
```

可用环境变量：

- `BUY_BOT_SLEEP_BETWEEN_ROUNDS=10`：买入机器人空转休眠秒数。
- `SELL_BOT_SLEEP_BETWEEN_ROUNDS=5`：卖出机器人空转休眠秒数，默认比买入更频繁。
- `B_BUY_WINDOW_START_LA=06:50`：B 策略默认避开开盘前 20 分钟。
- `B_MAX_BUY_UP_PCT=0.10`：当天涨幅超过 10% 不追。
- `B_UP_PCT_MAX=0.20`：筛选入池时，昨日涨幅超过 20% 不入池。
- `B_MAX_ENTRY_UP_PCT=0.04`：相对候选入选价涨幅超过 4% 不追。
- `B_MIN_PRICE=5.0`：低于 5 美元不买。
- `B_MAX_SPREAD_PCT=0.015`：买入价差超过 1.5% 不买。
- `B_MIN_AVG_DOLLAR_VOL20=20000000`：20 日均成交额低于 2000 万美元不买。
- `B_MAX_ACTIVE_POSITIONS=4`：B 策略同时持仓上限。
- `B_MAX_BELOW_OPEN_PCT=0.015`：实时价/限价低于当日开盘价超过 1.5% 不买。
- `B_MAX_PULLBACK_FROM_HIGH_PCT=0.03`：实时价/限价距离当日最高价回落超过 3% 不买。
- `B_REQUIRE_INTRADAY_VOLUME=1`：B 买入要求 `stock_operations.intraday_volume` 可用。
- `B_VOLUME_RATIO_EARLY=0.15`：06:50-07:30 要求今日累计量达到 20 日均量的 15%。
- `B_VOLUME_RATIO_MID=0.30`：07:30-09:30 要求今日累计量达到 20 日均量的 30%。
- `B_VOLUME_RATIO_LATE=0.45`：09:30 以后要求今日累计量达到 20 日均量的 45%。
- `B_SCORE_TOP_N=3`：B 买入每轮只记录评分前三。
- `B_SCORE_INTERVAL_MINUTES=5`：B 买入评分记录间隔，默认每 5 分钟一次。
- `B_SCORE_CONFIRMATIONS=3`：同一股票进入 Top3 满 3 次才允许买入。

### 本地调试流程（PyCharm）

1. 修改代码（PyCharm）
2. 构建并启动：

```bash
docker compose up -d --build
```
TradeBot#2026!
3. 查看日志：

```bash
docker compose logs -f tradebot
```

云端流程一致。
docker compose exec mysql mysql -u tradebot -p"$MYSQL_ROOT_PASSWORD" -e "SELECT NOW() as now_time, @@global.time_zone as gtz, @@session.time_zone as stz;"

### 本地盘中成交量同步

本地运行 Yahoo 数据同步，只更新 `stock_operations`，不写 `stock_prices_pool`：

```bash
DB_HOST=localhost DB_PORT=3307 DB_USER=tradebot DB_PASS='TradeBot#2026!' DB_NAME=cszy2000 \
OPS_VOLUME_STOCK_TYPES=A,B,F OPS_VOLUME_PERIOD=7d OPS_VOLUME_INTERVAL=1d \
OPS_VOLUME_START_LA=06:00 OPS_VOLUME_END_LA=17:00 OPS_VOLUME_SLEEP_SECONDS=300 \
.venv/bin/python app/sync_ops_intraday_volume.py
```

只跑一次：

```bash
OPS_VOLUME_RUN_ONCE=1 OPS_VOLUME_IGNORE_WINDOW=1 .venv/bin/python app/sync_ops_intraday_volume.py
```

写入字段只有一个：

- `intraday_volume`
