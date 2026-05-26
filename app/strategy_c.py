# -*- coding: utf-8 -*-
"""
兼容入口：策略 C 的期权价差逻辑已经迁移到 app.strategy_q。

保留这个文件是为了让旧脚本、旧环境变量和临时命令不立刻失效；
新的 Q 期权买入/卖出机器人请直接引用 app.strategy_q。
"""

from __future__ import annotations

import os

from app import strategy_q as _strategy_q
from app.strategy_q import *  # noqa: F401,F403


def __getattr__(name: str):
    return getattr(_strategy_q, name)


if __name__ == "__main__":
    strategy_Q_buy(os.getenv("C_TEST_SYMBOL", os.getenv("Q_TEST_SYMBOL", "QQQ")))
