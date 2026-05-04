# -*- coding: utf-8 -*-
"""
手动/定时记录券商持仓的收盘盈亏快照。

用途：
- 建议在美西时间 12:59 执行一次。
- 脚本会读取 Alpaca 当前持仓，把每只股票当前浮盈亏写入
  broker_position_close_snapshots 表。

示例：
    python app/capture_position_close_snapshot.py

说明：
- 默认强制记录，不依赖网页是否打开。
- 环境用 TRADE_ENV / ALPACA_MODE 决定 paper 或 live。
"""

from __future__ import annotations

from app import mobile_control as mc


def main():
    rows, err, _ = mc._get_positions_cached()
    if err:
        raise RuntimeError(err)

    mc._maybe_capture_close_snapshot(rows, force=True)
    print(
        f"[OK] captured close snapshot env={mc._trade_env()} positions={len(rows)} "
        f"table={mc.POSITION_CLOSE_TABLE}",
        flush=True,
    )


if __name__ == "__main__":
    main()
