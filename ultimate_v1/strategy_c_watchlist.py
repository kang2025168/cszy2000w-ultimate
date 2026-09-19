from __future__ import annotations

"""Canonical Strategy C long-term watchlist and database synchronizer."""

from dataclasses import dataclass

from .config import settings
from .db import db_conn
from .schema import ensure_schema


@dataclass(frozen=True)
class StrategyCWatchItem:
    symbol: str
    weight: float
    sector: str


STRATEGY_C_WATCHLIST: tuple[StrategyCWatchItem, ...] = (
    StrategyCWatchItem("MSFT", 0.07, "ai_platform"),
    StrategyCWatchItem("GOOGL", 0.05, "ai_platform"),
    StrategyCWatchItem("AMZN", 0.04, "ai_platform"),
    StrategyCWatchItem("META", 0.03, "ai_platform"),
    StrategyCWatchItem("PANW", 0.02, "cybersecurity"),
    StrategyCWatchItem("NVDA", 0.04, "ai_chip"),
    StrategyCWatchItem("AVGO", 0.04, "ai_chip"),
    StrategyCWatchItem("TSM", 0.04, "foundry"),
    StrategyCWatchItem("ASML", 0.04, "lithography"),
    StrategyCWatchItem("MU", 0.03, "memory"),
    StrategyCWatchItem("ISRG", 0.04, "robotics"),
    StrategyCWatchItem("TER", 0.02, "automation"),
    StrategyCWatchItem("LLY", 0.04, "healthcare"),
    StrategyCWatchItem("TMO", 0.05, "life_science"),
    StrategyCWatchItem("VRTX", 0.04, "biotech"),
    StrategyCWatchItem("BRK.B", 0.08, "financial"),
    StrategyCWatchItem("V", 0.04, "payments"),
    StrategyCWatchItem("CME", 0.03, "exchange"),
    StrategyCWatchItem("ETN", 0.05, "power"),
    StrategyCWatchItem("LIN", 0.04, "industrial_gas"),
    StrategyCWatchItem("CEG", 0.03, "nuclear_power"),
    StrategyCWatchItem("GEV", 0.03, "grid_equipment"),
    StrategyCWatchItem("SPCX", 0.02, "space"),
    StrategyCWatchItem("COST", 0.05, "consumer"),
    StrategyCWatchItem("WM", 0.04, "defensive"),
)


def validate_strategy_c_watchlist() -> None:
    symbols = [item.symbol for item in STRATEGY_C_WATCHLIST]
    if len(symbols) != 25:
        raise ValueError(f"Strategy C watchlist must contain 25 symbols, got {len(symbols)}")
    if len(set(symbols)) != len(symbols):
        raise ValueError("Strategy C watchlist contains duplicate symbols")
    total_weight = sum(item.weight for item in STRATEGY_C_WATCHLIST)
    if abs(total_weight - 1.0) > 1e-9:
        raise ValueError(f"Strategy C weights must total 1.0, got {total_weight:.8f}")


def sync_strategy_c_watchlist(*, dry_run: bool = False, prune_legacy: bool = True) -> dict:
    """Replace the C candidate pool while protecting active broker holdings."""
    validate_strategy_c_watchlist()
    ensure_schema()
    table = settings().ops_table
    if not table.replace("_", "").isalnum():
        raise ValueError(f"Invalid operations table name: {table!r}")

    stats = {
        "inserted": 0,
        "updated": 0,
        "held_preserved": 0,
        "deleted_operations": 0,
        "deleted_holding_rows": 0,
        "protected_active": [],
        "symbols": [],
    }
    with db_conn() as conn:
        with conn.cursor() as cur:
            for item in STRATEGY_C_WATCHLIST:
                cur.execute(
                    f"""
                    SELECT id, is_bought, qty
                    FROM `{table}`
                    WHERE UPPER(stock_code)=%s AND UPPER(stock_type)='C'
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (item.symbol,),
                )
                existing = cur.fetchone()
                action = "updated" if existing else "inserted"
                stats["symbols"].append(
                    {
                        "symbol": item.symbol,
                        "weight": item.weight,
                        "sector": item.sector,
                        "action": action,
                    }
                )
                if dry_run:
                    stats[action] += 1
                    continue

                note = f"C:WATCHLIST {item.sector} weight={item.weight:.2%}"[:80]
                if existing:
                    held = bool(int(existing.get("is_bought") or 0)) or abs(float(existing.get("qty") or 0)) > 0
                    cur.execute(
                        f"""
                        UPDATE `{table}`
                        SET stock_type='C',
                            strategy_group='C',
                            capital_pool='C',
                            weight=%s,
                            margin_used=0,
                            can_buy=CASE WHEN COALESCE(is_bought,0)=0 THEN 1 ELSE can_buy END,
                            can_sell=CASE WHEN COALESCE(is_bought,0)=0 THEN 0 ELSE can_sell END,
                            last_order_intent=CASE
                                WHEN COALESCE(is_bought,0)=0
                                 AND (last_order_id IS NULL OR last_order_id='')
                                THEN %s
                                ELSE last_order_intent
                            END,
                            updated_at=CURRENT_TIMESTAMP
                        WHERE id=%s
                        """,
                        (item.weight, note, existing["id"]),
                    )
                    stats["updated"] += 1
                    if held:
                        stats["held_preserved"] += 1
                    continue

                cur.execute(
                    f"""
                    INSERT INTO `{table}` (
                        stock_code, stock_type, weight,
                        is_bought, can_buy, can_sell, qty,
                        strategy_group, capital_pool, margin_used,
                        ac_t_enabled, ac_t_type, ac_t_state,
                        last_order_intent, created_at, updated_at
                    ) VALUES (
                        %s, 'C', %s,
                        0, 1, 0, 0,
                        'C', 'C', 0,
                        0, NULL, 'IDLE',
                        %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    )
                    """,
                    (item.symbol, item.weight, note),
                )
                stats["inserted"] += 1

            if prune_legacy:
                symbols = tuple(item.symbol for item in STRATEGY_C_WATCHLIST)
                placeholders = ", ".join(["%s"] * len(symbols))
                cur.execute(
                    f"""
                    SELECT so.id, UPPER(so.stock_code) AS symbol,
                           EXISTS(
                               SELECT 1
                               FROM position_holdings ph
                               WHERE UPPER(ph.symbol)=UPPER(so.stock_code)
                                 AND ph.status='open'
                                 AND ABS(COALESCE(ph.qty,0)) > 0
                           ) AS has_open_holding
                    FROM `{table}` so
                    WHERE UPPER(COALESCE(NULLIF(so.strategy_group,''), so.stock_type))='C'
                      AND UPPER(so.stock_code) NOT IN ({placeholders})
                    ORDER BY so.id
                    """,
                    symbols,
                )
                legacy_operations = list(cur.fetchall() or [])
                deletable_ids = []
                for row in legacy_operations:
                    if int(row.get("has_open_holding") or 0) == 1:
                        stats["protected_active"].append(str(row.get("symbol") or ""))
                    else:
                        deletable_ids.append(int(row["id"]))

                stats["deleted_operations"] = len(deletable_ids)
                if deletable_ids and not dry_run:
                    delete_placeholders = ", ".join(["%s"] * len(deletable_ids))
                    cur.execute(
                        f"DELETE FROM `{table}` WHERE id IN ({delete_placeholders})",
                        tuple(deletable_ids),
                    )

                cur.execute(
                    f"""
                    SELECT id, UPPER(symbol) AS symbol,
                           CASE WHEN status='open' AND ABS(COALESCE(qty,0)) > 0 THEN 1 ELSE 0 END AS active
                    FROM position_holdings
                    WHERE UPPER(COALESCE(NULLIF(strategy_group,''), stock_type))='C'
                      AND UPPER(symbol) NOT IN ({placeholders})
                    """,
                    symbols,
                )
                stale_holding_ids = []
                for row in cur.fetchall() or []:
                    symbol = str(row.get("symbol") or "")
                    if int(row.get("active") or 0) == 1:
                        if symbol not in stats["protected_active"]:
                            stats["protected_active"].append(symbol)
                    else:
                        stale_holding_ids.append(int(row["id"]))
                stats["deleted_holding_rows"] = len(stale_holding_ids)
                if stale_holding_ids and not dry_run:
                    delete_placeholders = ", ".join(["%s"] * len(stale_holding_ids))
                    cur.execute(
                        f"DELETE FROM position_holdings WHERE id IN ({delete_placeholders})",
                        tuple(stale_holding_ids),
                    )

        if dry_run:
            conn.rollback()
    stats["count"] = len(STRATEGY_C_WATCHLIST)
    stats["weight_total"] = sum(item.weight for item in STRATEGY_C_WATCHLIST)
    stats["dry_run"] = dry_run
    stats["prune_legacy"] = prune_legacy
    return stats
