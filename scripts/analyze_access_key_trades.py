# -*- coding: utf-8 -*-
"""
只统计 Alpaca access_key 已平仓交易表现：
- 只统计已经买入并卖出的交易
- 不统计未平仓仓位
- 统计胜率、总盈亏、总盈利、总亏损、盈亏比、Profit Factor
- 按股票统计已平仓盈亏
- 输出 TXT + CSV
"""

import os
import csv
import requests
from datetime import datetime, timezone
from collections import defaultdict, deque


TRADE_ENV = (os.getenv("TRADE_ENV") or os.getenv("ALPACA_MODE") or "paper").strip().lower()

if TRADE_ENV == "live":
    APCA_API_KEY_ID = os.getenv("LIVE_APCA_API_KEY_ID") or os.getenv("APCA_API_KEY_ID") or ""
    APCA_API_SECRET_KEY = os.getenv("LIVE_APCA_API_SECRET_KEY") or os.getenv("APCA_API_SECRET_KEY") or ""
    BASE_URL = os.getenv("LIVE_ALPACA_BASE_URL", "https://api.alpaca.markets")
else:
    APCA_API_KEY_ID = os.getenv("PAPER_APCA_API_KEY_ID") or os.getenv("APCA_API_KEY_ID") or ""
    APCA_API_SECRET_KEY = os.getenv("PAPER_APCA_API_SECRET_KEY") or os.getenv("APCA_API_SECRET_KEY") or ""
    BASE_URL = os.getenv("PAPER_ALPACA_BASE_URL", "https://paper-api.alpaca.markets")

AFTER = os.getenv("ANALYZE_AFTER", "2026-04-01T00:00:00Z")
UNTIL = os.getenv("ANALYZE_UNTIL", datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
SOURCE_FILTER = os.getenv("SOURCE_FILTER", "access_key")

REPORT_TXT = "/app/data/closed_trade_report.txt"
DETAIL_CSV = "/app/data/closed_trade_detail.csv"
SYMBOL_CSV = "/app/data/closed_trade_by_symbol.csv"


def headers():
    return {
        "APCA-API-KEY-ID": APCA_API_KEY_ID,
        "APCA-API-SECRET-KEY": APCA_API_SECRET_KEY,
    }


def fnum(x, default=0.0):
    try:
        if x is None or str(x).strip() == "":
            return default
        return float(x)
    except Exception:
        return default


def get_orders():
    url = f"{BASE_URL}/v2/orders"
    params = {
        "status": "all",
        "limit": 500,
        "after": AFTER,
        "until": UNTIL,
        "direction": "asc",
        "nested": "false",
    }

    all_orders = []
    page_token = None

    while True:
        if page_token:
            params["page_token"] = page_token

        r = requests.get(url, headers=headers(), params=params, timeout=25)
        if r.status_code != 200:
            raise RuntimeError(f"orders api error {r.status_code}: {r.text}")

        rows = r.json() or []
        all_orders.extend(rows)

        if len(rows) < 500:
            break

        page_token = rows[-1].get("id")
        if not page_token:
            break

    return all_orders


def main():
    if not APCA_API_KEY_ID or not APCA_API_SECRET_KEY:
        raise RuntimeError("缺少 Alpaca API Key")

    print("=" * 80)
    print(f"[INFO] env={TRADE_ENV}")
    print(f"[INFO] base_url={BASE_URL}")
    print(f"[INFO] after={AFTER}")
    print(f"[INFO] until={UNTIL}")
    print(f"[INFO] source_filter={SOURCE_FILTER}")
    print("=" * 80)

    orders = get_orders()

    filled = []
    has_source_field = False

    for o in orders:
        if o.get("status") != "filled":
            continue

        src = o.get("source")
        if src is not None:
            has_source_field = True

        if has_source_field and SOURCE_FILTER:
            if str(src) != SOURCE_FILTER:
                continue

        symbol = (o.get("symbol") or "").upper()
        side = (o.get("side") or "").lower()
        qty = fnum(o.get("filled_qty"))
        price = fnum(o.get("filled_avg_price"))
        filled_at = o.get("filled_at") or o.get("submitted_at")

        if not symbol or side not in ("buy", "sell") or qty <= 0 or price <= 0:
            continue

        filled.append({
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "price": price,
            "amount": qty * price,
            "filled_at": filled_at,
            "source": src,
            "order_id": o.get("id"),
        })

    if not has_source_field:
        print("[WARN] Alpaca 返回里没有 source 字段，本次统计所有 filled 订单。")

    lots = defaultdict(deque)
    closed_trades = []

    buy_count = 0
    sell_count = 0
    buy_amount = 0.0
    sell_amount = 0.0
    unmatched_sell_qty = defaultdict(float)

    for o in filled:
        sym = o["symbol"]
        side = o["side"]
        qty = o["qty"]
        price = o["price"]

        if side == "buy":
            buy_count += 1
            buy_amount += qty * price
            lots[sym].append({
                "qty": qty,
                "price": price,
                "time": o["filled_at"],
                "order_id": o["order_id"],
            })

        elif side == "sell":
            sell_count += 1
            sell_amount += qty * price

            remain = qty

            while remain > 0 and lots[sym]:
                lot = lots[sym][0]
                match_qty = min(remain, lot["qty"])

                buy_price = lot["price"]
                sell_price = price
                buy_cost = buy_price * match_qty
                sell_value = sell_price * match_qty
                pnl = sell_value - buy_cost
                pnl_pct = pnl / buy_cost if buy_cost > 0 else 0.0

                closed_trades.append({
                    "symbol": sym,
                    "qty": match_qty,
                    "buy_price": buy_price,
                    "sell_price": sell_price,
                    "buy_cost": buy_cost,
                    "sell_value": sell_value,
                    "pnl": pnl,
                    "pnl_pct": pnl_pct,
                    "buy_time": lot["time"],
                    "sell_time": o["filled_at"],
                })

                lot["qty"] -= match_qty
                remain -= match_qty

                if lot["qty"] <= 0:
                    lots[sym].popleft()

            if remain > 0:
                unmatched_sell_qty[sym] += remain

    total_pnl = sum(t["pnl"] for t in closed_trades)
    total_buy_cost = sum(t["buy_cost"] for t in closed_trades)
    total_sell_value = sum(t["sell_value"] for t in closed_trades)

    wins = [t for t in closed_trades if t["pnl"] > 0]
    losses = [t for t in closed_trades if t["pnl"] < 0]
    flats = [t for t in closed_trades if t["pnl"] == 0]

    gross_profit = sum(t["pnl"] for t in wins)
    gross_loss = abs(sum(t["pnl"] for t in losses))

    win_rate = len(wins) / len(closed_trades) * 100 if closed_trades else 0.0
    loss_rate = len(losses) / len(closed_trades) * 100 if closed_trades else 0.0
    realized_return_pct = total_pnl / total_buy_cost * 100 if total_buy_cost > 0 else 0.0

    profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0.0
    avg_pnl = total_pnl / len(closed_trades) if closed_trades else 0.0
    avg_win = gross_profit / len(wins) if wins else 0.0
    avg_loss = -gross_loss / len(losses) if losses else 0.0
    payoff_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else 0.0

    best_trade = max(closed_trades, key=lambda x: x["pnl"], default=None)
    worst_trade = min(closed_trades, key=lambda x: x["pnl"], default=None)

    by_symbol = defaultdict(lambda: {
        "trades": 0,
        "wins": 0,
        "losses": 0,
        "qty": 0.0,
        "buy_cost": 0.0,
        "sell_value": 0.0,
        "pnl": 0.0,
    })

    for t in closed_trades:
        s = by_symbol[t["symbol"]]
        s["trades"] += 1
        s["qty"] += t["qty"]
        s["buy_cost"] += t["buy_cost"]
        s["sell_value"] += t["sell_value"]
        s["pnl"] += t["pnl"]

        if t["pnl"] > 0:
            s["wins"] += 1
        elif t["pnl"] < 0:
            s["losses"] += 1

    symbol_rows = []
    for sym, s in by_symbol.items():
        wr = s["wins"] / s["trades"] * 100 if s["trades"] else 0.0
        ret_pct = s["pnl"] / s["buy_cost"] * 100 if s["buy_cost"] > 0 else 0.0
        symbol_rows.append({
            "symbol": sym,
            "trades": s["trades"],
            "wins": s["wins"],
            "losses": s["losses"],
            "win_rate": wr,
            "qty": s["qty"],
            "buy_cost": s["buy_cost"],
            "sell_value": s["sell_value"],
            "pnl": s["pnl"],
            "return_pct": ret_pct,
        })

    symbol_rows.sort(key=lambda x: x["pnl"], reverse=True)

    with open(REPORT_TXT, "w", encoding="utf-8") as f:
        f.write("=" * 80 + "\n")
        f.write("Alpaca access_key 已平仓交易统计报告\n")
        f.write("=" * 80 + "\n")
        f.write(f"账户环境: {TRADE_ENV}\n")
        f.write(f"统计开始: {AFTER}\n")
        f.write(f"统计结束: {UNTIL}\n")
        f.write(f"订单来源筛选: {SOURCE_FILTER}\n\n")

        f.write("========== 已平仓总览 ==========\n")
        f.write(f"filled订单数: {len(filled)}\n")
        f.write(f"买单数: {buy_count}\n")
        f.write(f"卖单数: {sell_count}\n")
        f.write(f"买入总金额: ${buy_amount:,.2f}\n")
        f.write(f"卖出总金额: ${sell_amount:,.2f}\n")
        f.write(f"已平仓匹配笔数: {len(closed_trades)}\n")
        f.write(f"盈利笔数: {len(wins)}\n")
        f.write(f"亏损笔数: {len(losses)}\n")
        f.write(f"打平笔数: {len(flats)}\n")
        f.write(f"胜率: {win_rate:.2f}%\n")
        f.write(f"亏损率: {loss_rate:.2f}%\n\n")

        f.write("========== 已平仓盈亏 ==========\n")
        f.write(f"已实现盈亏: ${total_pnl:,.2f}\n")
        f.write(f"已平仓买入成本: ${total_buy_cost:,.2f}\n")
        f.write(f"已平仓卖出金额: ${total_sell_value:,.2f}\n")
        f.write(f"已实现收益率: {realized_return_pct:.2f}%\n")
        f.write(f"总盈利: ${gross_profit:,.2f}\n")
        f.write(f"总亏损: -${gross_loss:,.2f}\n")
        f.write(f"Profit Factor: {profit_factor:.2f}\n")
        f.write(f"平均每笔盈亏: ${avg_pnl:,.2f}\n")
        f.write(f"平均盈利: ${avg_win:,.2f}\n")
        f.write(f"平均亏损: ${avg_loss:,.2f}\n")
        f.write(f"盈亏比: {payoff_ratio:.2f}\n\n")

        f.write("========== 最大单笔 ==========\n")
        if best_trade:
            f.write(
                f"最大盈利: {best_trade['symbol']} qty={best_trade['qty']:.0f} "
                f"buy={best_trade['buy_price']:.2f} sell={best_trade['sell_price']:.2f} "
                f"pnl=${best_trade['pnl']:,.2f} return={best_trade['pnl_pct']*100:.2f}%\n"
            )
        if worst_trade:
            f.write(
                f"最大亏损: {worst_trade['symbol']} qty={worst_trade['qty']:.0f} "
                f"buy={worst_trade['buy_price']:.2f} sell={worst_trade['sell_price']:.2f} "
                f"pnl=${worst_trade['pnl']:,.2f} return={worst_trade['pnl_pct']*100:.2f}%\n"
            )
        f.write("\n")

        f.write("========== 按股票统计：只含已平仓 ==========\n")
        for r in symbol_rows:
            f.write(
                f"{r['symbol']:8s} "
                f"trades={r['trades']:3d} "
                f"win_rate={r['win_rate']:6.2f}% "
                f"buy_cost=${r['buy_cost']:10.2f} "
                f"sell_value=${r['sell_value']:10.2f} "
                f"pnl=${r['pnl']:10.2f} "
                f"return={r['return_pct']:7.2f}%\n"
            )

        if unmatched_sell_qty:
            f.write("\n========== 警告：卖出找不到买入 ==========\n")
            for sym, q in unmatched_sell_qty.items():
                f.write(f"{sym}: unmatched_sell_qty={q:.4f}\n")

        f.write("\n========== 简单判断 ==========\n")
        if profit_factor >= 2 and win_rate >= 50 and total_pnl > 0:
            f.write("已平仓交易结构偏强：胜率、盈亏比、Profit Factor 都不错。\n")
        elif total_pnl > 0:
            f.write("已平仓交易赚钱，但结构还需要继续优化。\n")
        else:
            f.write("已平仓交易亏损，需要重点检查买入追高和止损逻辑。\n")

    with open(DETAIL_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "symbol", "qty", "buy_price", "sell_price",
            "buy_cost", "sell_value", "pnl", "pnl_pct",
            "buy_time", "sell_time"
        ])
        for t in closed_trades:
            writer.writerow([
                t["symbol"],
                round(t["qty"], 4),
                round(t["buy_price"], 4),
                round(t["sell_price"], 4),
                round(t["buy_cost"], 2),
                round(t["sell_value"], 2),
                round(t["pnl"], 2),
                round(t["pnl_pct"] * 100, 2),
                t["buy_time"],
                t["sell_time"],
            ])

    with open(SYMBOL_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "symbol", "trades", "wins", "losses", "win_rate",
            "qty", "buy_cost", "sell_value", "pnl", "return_pct"
        ])
        for r in symbol_rows:
            writer.writerow([
                r["symbol"],
                r["trades"],
                r["wins"],
                r["losses"],
                round(r["win_rate"], 2),
                round(r["qty"], 4),
                round(r["buy_cost"], 2),
                round(r["sell_value"], 2),
                round(r["pnl"], 2),
                round(r["return_pct"], 2),
            ])

    print("\n========== 已平仓总览 ==========")
    print(f"已平仓匹配笔数: {len(closed_trades)}")
    print(f"盈利笔数: {len(wins)} | 亏损笔数: {len(losses)}")
    print(f"胜率: {win_rate:.2f}%")
    print(f"已实现盈亏: ${total_pnl:,.2f}")
    print(f"总盈利: ${gross_profit:,.2f}")
    print(f"总亏损: -${gross_loss:,.2f}")
    print(f"Profit Factor: {profit_factor:.2f}")
    print(f"平均盈利: ${avg_win:.2f}")
    print(f"平均亏损: ${avg_loss:.2f}")
    print(f"盈亏比: {payoff_ratio:.2f}")

    print("\n✅ 文件已生成：")
    print(REPORT_TXT)
    print(DETAIL_CSV)
    print(SYMBOL_CSV)


if __name__ == "__main__":
    main()