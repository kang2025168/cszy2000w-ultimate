# -*- coding: utf-8 -*-
"""
F 卖出机器人入口。

只运行 F 策略卖出循环。它接管 F 持仓后的止损、分批止盈、
回撤保护和盘前盘后卖出管理。
"""

import os

os.environ.setdefault("BOT_PROCESS_NAME", "f_sell_bot")
os.environ["BOT_STRATEGIES"] = "F"
os.environ.setdefault("BOT_SLEEP_BETWEEN_ROUNDS", "60")

from app.bots.split_core import main_loop


if __name__ == "__main__":
    main_loop("sell")
