# -*- coding: utf-8 -*-
"""
B 买入机器人入口。

只运行 B 策略买入循环。它复用 split_core 的通用买入调度，
但通过 BOT_STRATEGIES=B 保证这个进程只扫描和执行 B 买入。
"""

import os

os.environ.setdefault("BOT_PROCESS_NAME", "b_buy_bot")
os.environ["BOT_STRATEGIES"] = "B"

from app.bots.split_core import main_loop


if __name__ == "__main__":
    main_loop("buy")
