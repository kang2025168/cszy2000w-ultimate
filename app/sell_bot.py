# -*- coding: utf-8 -*-
"""Independent sell bot entrypoint."""
import os

os.environ.setdefault("BOT_PROCESS_NAME", "sell_bot")

from app.bots.split_core import main_loop


if __name__ == "__main__":
    main_loop("sell")
