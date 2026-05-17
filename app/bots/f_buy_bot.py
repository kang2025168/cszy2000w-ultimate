# -*- coding: utf-8 -*-
"""
F 买入机器人入口。

只运行 F 策略买入循环。F 负责扫描 B 卖出后的 monster_watchlist，
确认二次拉回后写入并执行 F 买入。
"""

import os

os.environ.setdefault("BOT_PROCESS_NAME", "f_buy_bot")
os.environ["BOT_STRATEGIES"] = "F"
os.environ.setdefault("BOT_SLEEP_BETWEEN_ROUNDS", "60")

from app.bots.split_core import main_loop


if __name__ == "__main__":
    main_loop("buy")
