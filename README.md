## 运行方式（Docker Compose）

本项目通过 Docker Compose 运行，所有配置来自 `.env`。

### 关键入口

- 主服务：`tradebot`
- 主入口：`app/trade_bot_main.py`
- 策略 A 买卖逻辑：`app/strategy_a.py`

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