# -*- coding: utf-8 -*-
"""
B 卖出机器人入口。

只运行 B 策略卖出循环。它负责 B 持仓的止损、止盈、
盘前盘后保护等卖出侧逻辑，不参与任何买入。
"""

import os

os.environ.setdefault("BOT_PROCESS_NAME", "b_sell_bot")
os.environ["BOT_STRATEGIES"] = "B"

from app.bots.split_core import main_loop


if __name__ == "__main__":
    main_loop("sell")
