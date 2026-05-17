"""
机器人入口包。

这里放所有独立机器人入口和共享调度核心：
- b_buy_bot / b_sell_bot：B 策略买卖独立进程
- f_buy_bot / f_sell_bot：F 策略买卖独立进程
- split_core：买卖机器人共用的扫描、分发、循环逻辑
"""
