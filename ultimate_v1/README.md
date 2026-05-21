# CSZY Ultimate V1

这是一个干净版 V1 系统，目标是把交易从“很多脚本各跑各的”改造成“多个机器人听中央状态”的结构。

核心目标：

- 看板机器人管理资金、仓位、资金池和中央状态。
- 风险机器人计算风险，影响看板机器人。
- A/C 共用一个低频机器人。
- B 买入和 B 卖出分离。
- D 买入和 D 卖出分离，D 卖出负责收盘前强制平仓。
- 网页看板只负责展示和后续人工控制，不直接塞交易逻辑。

## 机器人架构

```text
risk_bot
  写 risk_state
  ↓
dashboard_bot
  读取 Alpaca / stock_operations / position_holdings / risk_state
  写 capital_state / bot_heartbeats
  ↓
ac_bot
b_buy_bot
b_sell_bot
d_buy_bot
d_sell_bot
  ↓
position_holdings / stock_operations / Alpaca
```

## 机器人职责

`dashboard_bot`

看板机器人，负责管理资金和仓位状态。它会同步 Alpaca 持仓，计算 A/B/C/D 目标资金、已用资金、可用资金，并写入 `capital_state`。买入机器人必须听它。

`risk_bot`

风险机器人，负责计算单日亏损、连续亏损、最大回撤、`risk_multiplier`、是否禁止 B/D 买入，并写入 `risk_state`。它影响看板机器人，不直接乱卖 A/C 长期仓。

`ac_bot`

A/C 低频买卖机器人。A 是指数底仓，C 是长期优质股。它们节奏慢，所以共用一个机器人。当前是中文伪代码占位，只做风控和资金检查，不真实下单。

`b_buy_bot`

B 买入机器人。只负责 B 类进攻买入，必须经过风控和 B 资金池检查。当前入口会调用旧项目的 `app.strategy_b.strategy_B_buy`。

`b_sell_bot`

B 卖出机器人。只负责 B 类持仓退出、动态止损、b_stage、pending stop 等。卖出不受资金池限制。

`d_buy_bot`

D 买入机器人。只负责日内开仓，只能使用 D 资金池。SAFE / RISK_OFF 或接近收盘时应禁止新开仓。

`d_sell_bot`

D 卖出机器人。负责 D 类止盈止损和收盘前强制平仓，优先级最高。

`web_app`

网页看板，展示账户、资金池、风险状态、持仓、机器人心跳。地址：

```text
http://127.0.0.1:8060
```

## 中央状态表

`risk_state`

保存风险机器人输出的最新风险状态。

`capital_state`

保存看板机器人输出的 A/B/C/D 资金池状态。

`bot_heartbeats`

保存每个机器人的心跳，网页可以看到哪个机器人正在工作。

`bot_commands`

预留给网页按钮或人工指令，例如暂停 B 买入、强制 D 清仓、刷新持仓。

## 启动

启动网页看板：

```bash
docker compose --profile ultimate up -d ultimate_v1
```

单次刷新风险状态：

```bash
docker compose exec -T ultimate_v1 python -m app.bots.risk_bot
```

单次刷新看板资金/仓位状态：

```bash
docker compose exec -T ultimate_v1 python -m app.bots.dashboard_bot
```

循环运行风险机器人：

```bash
docker compose exec -T ultimate_v1 python -m app.bots.risk_bot --loop --interval 60
```

循环运行看板机器人：

```bash
docker compose exec -T ultimate_v1 python -m app.bots.dashboard_bot --loop --interval 300
```

## 策略机器人命令

```bash
docker compose exec -T ultimate_v1 python -m app.bots.ac_bot scan
docker compose exec -T ultimate_v1 python -m app.bots.ac_bot buy --group A --symbol QQQ
docker compose exec -T ultimate_v1 python -m app.bots.ac_bot buy --group C --symbol MSFT

docker compose exec -T ultimate_v1 ./scripts/run.sh b_buy_bot
docker compose exec -T ultimate_v1 ./scripts/run.sh b_sell_bot
docker compose exec -T ultimate_v1 ./scripts/run.sh f_buy_bot
docker compose exec -T ultimate_v1 ./scripts/run.sh f_sell_bot

docker compose exec -T ultimate_v1 python -m app.bots.d_buy_bot QQQ
docker compose exec -T ultimate_v1 python -m app.bots.d_sell_bot
docker compose exec -T ultimate_v1 python -m app.bots.d_sell_bot --flatten
```

也可以通过统一调度器：

```bash
docker compose exec -T ultimate_v1 python -m ultimate_v1.strategy_runner B buy TSLA
docker compose exec -T ultimate_v1 python -m ultimate_v1.strategy_runner D flatten
```

## 机器人运行查看和排障命令

线上网页开关拉起的机器人，默认都作为 `ultimate_v1` 容器里的子进程运行，由 `ultimate_v1.web_app` 主管进程管理。也就是说，网页里打开 `b_buy_bot` 后，真正出现的进程通常在 `cszy_ultimate_v1` 容器里，而不是单独的 `buybot` 容器。

### 一眼看所有机器人进程

容器里可能没有 `ps` 命令，所以优先在宿主机上使用 `docker top`：

```bash
docker top cszy_ultimate_v1
```

只看机器人子进程：

```bash
docker top cszy_ultimate_v1 | grep -aE "app.bots"
```

只看买卖机器人：

```bash
docker top cszy_ultimate_v1 | grep -aE "app.bots.(b_buy_bot|b_sell_bot|f_buy_bot|f_sell_bot|d_buy_bot|d_sell_bot)"
```

只看 B 买入 / B 卖出：

```bash
docker top cszy_ultimate_v1 | grep -aE "app.bots.(b_buy_bot|b_sell_bot)"
```

看到下面这种行，说明对应机器人真的在跑：

```text
/usr/local/bin/python -u -m app.bots.b_buy_bot
/usr/local/bin/python -u -m app.bots.b_sell_bot
/usr/local/bin/python -u -m app.bots.d_buy_bot --loop --interval 30
```

### 看所有机器人实时日志

```bash
docker compose logs -f ultimate_v1 | grep -aE "BOT|bot|REBALANCE|RISK|DASHBOARD|BUY|SELL"
```

`grep -a` 很重要。Docker 日志偶尔会让 `grep` 误判为二进制流，如果不用 `-a`，可能只显示：

```text
grep: (standard input): binary file matches
```

### 看最近一段时间日志

最近 10 分钟：

```bash
docker compose logs --since 10m ultimate_v1 | grep -aE "BOT|bot|REBALANCE|RISK|DASHBOARD|BUY|SELL"
```

最近 1 小时：

```bash
docker compose logs --since 1h ultimate_v1 | grep -aE "BOT|bot|REBALANCE|RISK|DASHBOARD|BUY|SELL"
```

今天 0 点以后：

```bash
docker compose logs --since "$(date +%Y-%m-%d)T00:00:00" ultimate_v1 | grep -aE "BOT|bot|REBALANCE|RISK|DASHBOARD|BUY|SELL"
```

从现在开始持续看新日志：

```bash
docker compose logs --since 30s -f ultimate_v1 | grep -aE "BOT|bot|REBALANCE|RISK|DASHBOARD|BUY|SELL"
```

### 看网页主管启动/关闭机器人记录

```bash
docker compose logs --since 24h ultimate_v1 | grep -aE "BOT SUPERVISOR|started|stopped"
```

只看某个机器人，例如 B 买入：

```bash
docker compose logs --since 24h ultimate_v1 | grep -aE "BOT SUPERVISOR|started b_buy_bot|stopped b_buy_bot|b_buy_bot"
```

### 看 B 机器人买卖日志

B 买入和 B 卖出在日志里常见前缀：

- `b_buy_bot`：主管启动/关闭时会出现。
- `b_sell_bot`：主管启动/关闭时会出现。
- `[BUY BOT]`：买入循环日志，B/F 买入共用这个前缀。
- `[SELL BOT]`：卖出循环日志，B/F 卖出共用这个前缀。
- `[B BUY]`：B 策略买入细节。
- `[B SELL]`：B 策略卖出细节。

实时看 B 买卖：

```bash
docker compose logs -f ultimate_v1 | grep -aE "b_buy_bot|b_sell_bot|\[BUY BOT\]|\[SELL BOT\]|\[B BUY\]|\[B SELL\]"
```

看今天的 B 买卖：

```bash
docker compose logs --since "$(date +%Y-%m-%d)T00:00:00" ultimate_v1 | grep -aE "b_buy_bot|b_sell_bot|\[BUY BOT\]|\[SELL BOT\]|\[B BUY\]|\[B SELL\]"
```

只看 B 买入是否启动：

```bash
docker compose logs --since 24h ultimate_v1 | grep -aE "started b_buy_bot|stopped b_buy_bot|b_buy_bot|\[BUY BOT\]|\[B BUY\]"
```

只看 B 卖出是否启动：

```bash
docker compose logs --since 24h ultimate_v1 | grep -aE "started b_sell_bot|stopped b_sell_bot|b_sell_bot|\[SELL BOT\]|\[B SELL\]"
```

注意：纯 `[BUY BOT] market closed, sleep 60s` 只说明“买入角色循环活着，但当前判断为非交易时段”。它不等于已经买入，也不一定单独代表 B，因为 F 买入也复用 `[BUY BOT]` 前缀。要确认是不是 B 买入进程，配合 `docker top` 看 `app.bots.b_buy_bot`。

### 看 D 机器人日志

```bash
docker compose logs -f ultimate_v1 | grep -aE "d_buy_bot|d_sell_bot|\[D BUY BOT\]|\[D SELL BOT\]|\[D FLATTEN\]"
```

D 买入常见占位日志：

```text
[D BUY BOT] 扫描占位：后续接日内信号；接近收盘禁止新开仓
```

这表示 `d_buy_bot` 在运行，但当前没有具体 symbol 信号触发买入。

### 看 rebalance / risk / dashboard 日志

```bash
docker compose logs -f ultimate_v1 | grep -aE "rebalance_bot|risk_bot|dashboard_bot|REBALANCE|RISK|DASHBOARD"
```

### 查看网页接口里的机器人状态

网页看板需要登录，最简单是直接在页面右上机器人卡片看状态。命令行更推荐看数据库心跳和控制表。

查看机器人心跳：

```bash
docker compose exec mysql sh -lc 'mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" -e "SELECT bot_name,status,last_seen_at,last_message FROM bot_heartbeats ORDER BY bot_name;"'
```

查看网页机器人开关：

```bash
docker compose exec mysql sh -lc 'mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" -e "SELECT bot_name,enabled,updated_at FROM bot_controls ORDER BY bot_name;"'
```

查看机器人启动/关闭事件，这些事件也会显示在网页“今日交易记录”里：

```bash
docker compose exec mysql sh -lc 'mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" -e "SELECT created_at,bot_name,action,status,pid,message FROM bot_lifecycle_events ORDER BY id DESC LIMIT 30;"'
```

### 查看旧策略控制开关

网页机器人开关只控制“进程是否启动”。B/F 买入是否真的允许，还要看旧控制表 `bot_control`。

查看旧控制表：

```bash
docker compose exec mysql sh -lc 'mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" -e "SELECT * FROM bot_control\G"'
```

日志里如果看到：

```text
[BUY BOT] skipped: strategy_b_enabled=0
```

说明 B 买入进程是活的，但旧控制表里 B 策略买入关闭。

打开全局买入和 B 策略买入：

```bash
docker compose exec mysql sh -lc 'mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" -e "UPDATE bot_control SET global_buy_enabled=1, strategy_b_enabled=1 WHERE id=1;"'
```

关闭全局买入和 B 策略买入：

```bash
docker compose exec mysql sh -lc 'mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" -e "UPDATE bot_control SET global_buy_enabled=0, strategy_b_enabled=0 WHERE id=1;"'
```

只打开 B 策略，但不打开全局买入，仍然不会买：

```text
global_buy=0 B=1
```

要允许 B 买入，通常需要：

```text
global_buy=1 B=1
```

### 开关机器人进程

推荐在网页看板里开关机器人，因为网页主管会同步心跳和生命周期记录。

如果必须用命令行改网页开关表，只是改数据库状态，不一定马上启动/停止当前进程；下一次网页服务重启或同步时会生效：

```bash
docker compose exec mysql sh -lc 'mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" -e "UPDATE bot_controls SET enabled=1 WHERE bot_name=\"b_buy_bot\";"'
```

关闭 B 买入进程开关：

```bash
docker compose exec mysql sh -lc 'mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" -e "UPDATE bot_controls SET enabled=0 WHERE bot_name=\"b_buy_bot\";"'
```

改完数据库后，若需要立即按开关状态重新拉起或关闭所有机器人，可以重启网页主管容器：

```bash
docker compose restart ultimate_v1
```

重启后再确认：

```bash
docker top cszy_ultimate_v1 | grep -aE "app.bots"
```

### 常见日志含义

```text
[BUY BOT] market closed, sleep 60s
```

买入机器人活着，但当前判断为非交易时间，所以每 60 秒睡眠一次。

```text
[SELL BOT] market closed, sleep 60s
```

卖出机器人活着，但当前判断为非交易时间。

```text
[SELL BOT] FORCE phase real=afterhours_add effective=regular env=live
```

真实时间段是盘后 `afterhours_add`，但环境变量强制机器人按 `regular` 盘中逻辑跑。`env=live` 表示当前是 live 环境，要特别谨慎。

```text
[SELL BOT] loop round=8359 phase=regular emergency_stop=0 sell_only=0 global_buy=0 B=0 F=0
```

卖出机器人第 8359 轮循环。`round` 只是循环次数，不是交易次数。

- `emergency_stop=0`：没有紧急停止。
- `sell_only=0`：不是只卖模式。
- `global_buy=0`：全局买入关闭。
- `B=0`：B 策略关闭。
- `F=0`：F 策略关闭。

```text
[SELL BOT] round done phase=regular scanned=3 eligible=0 traded=0
```

本轮扫描 3 条记录，符合条件 0 条，交易 0 次。

```text
[BOT SUPERVISOR] started b_buy_bot pid=1020242
```

网页主管已经启动 B 买入机器人，进程号是 `1020242`。

```text
[BOT SUPERVISOR] stopped b_buy_bot
```

网页主管已经停止 B 买入机器人。

### 排障流程

如果网页显示绿灯，但怀疑没跑：

```bash
docker top cszy_ultimate_v1 | grep -aE "app.bots"
docker compose logs --since 10m ultimate_v1 | grep -aE "BOT SUPERVISOR|started|stopped|b_buy_bot|b_sell_bot|d_buy_bot|d_sell_bot"
```

如果日志有 `[BUY BOT]`，但查不到 B 买入：

```bash
docker top cszy_ultimate_v1 | grep -aE "app.bots.(b_buy_bot|f_buy_bot)"
```

如果 `docker top` 能看到 `f_buy_bot`，但没有 `b_buy_bot`，那 `[BUY BOT]` 可能来自 F 买入。

如果看到 `skipped: strategy_b_enabled=0`：

```bash
docker compose exec mysql sh -lc 'mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" -e "SELECT * FROM bot_control\G"'
```

确认 `global_buy_enabled` 和 `strategy_b_enabled` 是否都是 `1`。

如果日志一直是 `market closed`：

- 机器人进程是活的。
- 当前交易阶段被判断为闭市。
- 不会扫描买卖或下单。

如果怀疑有残留孤儿进程：

```bash
docker top cszy_ultimate_v1 | grep -aE "app.bots"
docker compose restart ultimate_v1
docker top cszy_ultimate_v1 | grep -aE "app.bots"
```

重启 `ultimate_v1` 会清掉容器里的子进程，然后网页主管会按 `bot_controls` 重新拉起启用的机器人。

## 代码接入规则

买入前必须经过统一检查：

```python
from ultimate_v1.trading_gate import can_open_position

allow, reason = can_open_position("B", estimated_notional=500)
if not allow:
    print(f"skip buy: {reason}")
```

买入成交后更新展示持仓：

```python
from ultimate_v1.position_holdings import upsert_buy_holding

upsert_buy_holding(
    "QQQ", "B", qty=10, avg_entry_price=420.12,
    current_price=420.12, stop_loss_price=407.5,
    capital_pool="B", margin_used=0, last_order_id="..."
)
```

卖出成交后关闭或更新展示持仓：

```python
from ultimate_v1.position_holdings import update_sell_holding

update_sell_holding(
    "QQQ", "B", sell_qty=10, sell_price=430.0,
    remaining_qty=0, last_order_id="..."
)
```

## 当前阶段

A/C/D 现在是中文伪代码占位，只做资金池和风控检查，不真实下单。

B 是适配器入口：先经过 Ultimate V1 的风控和资金池检查，再调用旧项目里的 `app.strategy_b.strategy_B_buy/sell`。这样既能把 B 纳入新系统，又不会一次性搬动旧 B 的复杂买卖逻辑。

如果在宿主机直接跑，而 `.env` 里 `DB_HOST=mysql`，宿主机通常解析不了这个名字；可以临时改成：

```bash
DB_HOST=127.0.0.1 DB_PORT=3307 python -m ultimate_v1.strategy_runner A buy QQQ
```

所有主要功能都可以通过 `.env` 开关关闭。
