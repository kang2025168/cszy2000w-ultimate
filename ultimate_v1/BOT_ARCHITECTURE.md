# Ultimate V1 机器人架构说明

## 总原则

这个系统不是让每个策略各自决定资金和风险，而是让所有机器人听中央状态。

买入机器人必须听：

```text
risk_state + capital_state + 自己的策略信号
```

卖出机器人优先听：

```text
持仓安全 + 策略退出规则 + 强制风控
```

资金池不能卡住卖出。

## 机器人列表

```text
dashboard_bot  看板机器人，管理资金和仓位状态
risk_bot       风险机器人，控制风险并影响看板
ac_bot         A/C 低频买卖机器人
b_buy_bot      B 买入机器人
b_sell_bot     B 卖出机器人
d_buy_bot      D 买入机器人
d_sell_bot     D 卖出和强平机器人
web_app        网页看板
```

## 数据流

```text
risk_bot
  ↓ 写 risk_state

dashboard_bot
  ↓ 读取 risk_state
  ↓ 同步 Alpaca 持仓
  ↓ 计算 A/B/C/D 资金池
  ↓ 写 capital_state

ac_bot / b_buy_bot / d_buy_bot
  ↓ 读取资金池和风控状态
  ↓ 允许才新开仓

b_sell_bot / d_sell_bot
  ↓ 管理已有持仓
  ↓ 触发止损、止盈、强平

web_app
  ↓ 展示所有状态
```

## 为什么这样拆

`dashboard_bot` 是账户驾驶舱，负责“现在还能不能买、每组还能用多少钱”。

`risk_bot` 是刹车系统，负责“今天是否亏太多、是否连亏、是否进入避险”。

`ac_bot` 低频，因为 A/C 是长期仓，不需要每分钟跑。

`b_buy_bot` 和 `b_sell_bot` 分开，因为 B 的买入是找机会，卖出是保护利润和控制回撤。

`d_buy_bot` 和 `d_sell_bot` 分开，因为 D 类不能隔夜，卖出和强平必须非常可靠。

## 当前实现状态

A/C/D 是可执行占位，不真实下单。

B 已经有适配器，会先过 Ultimate V1 总控，再调用旧项目的 B 策略。

下一步最应该做的是把 B 旧策略内部成交后的持仓更新接到 `position_holdings`，然后再逐步实现 A/C/D 的真实信号。
