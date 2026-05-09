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
docker compose exec -T ultimate_v1 python -m ultimate_v1.bots.risk_bot
```

单次刷新看板资金/仓位状态：

```bash
docker compose exec -T ultimate_v1 python -m ultimate_v1.bots.dashboard_bot
```

循环运行风险机器人：

```bash
docker compose exec -T ultimate_v1 python -m ultimate_v1.bots.risk_bot --loop --interval 60
```

循环运行看板机器人：

```bash
docker compose exec -T ultimate_v1 python -m ultimate_v1.bots.dashboard_bot --loop --interval 300
```

## 策略机器人命令

```bash
docker compose exec -T ultimate_v1 python -m ultimate_v1.bots.ac_bot scan
docker compose exec -T ultimate_v1 python -m ultimate_v1.bots.ac_bot buy --group A --symbol QQQ
docker compose exec -T ultimate_v1 python -m ultimate_v1.bots.ac_bot buy --group C --symbol MSFT

docker compose exec -T ultimate_v1 python -m ultimate_v1.bots.b_buy_bot TSLA
docker compose exec -T ultimate_v1 python -m ultimate_v1.bots.b_sell_bot TSLA

docker compose exec -T ultimate_v1 python -m ultimate_v1.bots.d_buy_bot QQQ
docker compose exec -T ultimate_v1 python -m ultimate_v1.bots.d_sell_bot
docker compose exec -T ultimate_v1 python -m ultimate_v1.bots.d_sell_bot --flatten
```

也可以通过统一调度器：

```bash
docker compose exec -T ultimate_v1 python -m ultimate_v1.strategy_runner B buy TSLA
docker compose exec -T ultimate_v1 python -m ultimate_v1.strategy_runner D flatten
```

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
