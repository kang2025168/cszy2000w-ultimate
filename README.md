## 运行方式（Docker Compose）

本项目通过 Docker Compose 运行，所有配置来自 `.env`。

### 关键入口

- 主服务：`tradebot`
- 主入口：`app/trade_bot_main.py`
- 独立买入机器人：`app/buy_bot.py`
- 独立卖出机器人：`app/sell_bot.py`
- 拆分后的共享运行层：`app/bots/split_core.py`
- 策略 A 买卖逻辑：`app/strategy_a.py`

### 买卖机器人拆分

买入和卖出可以分开运行：

```bash
./scripts/run.sh buy_bot
./scripts/run.sh sell_bot
```

Docker 里已经预留了 `split-bots` profile，默认不影响现有 `tradebot`：

```bash
docker compose --profile split-bots up -d --build buybot sellbot
```

可用环境变量：

- `BOT_STRATEGIES=B,F`：控制拆分机器人扫描哪些策略。
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
