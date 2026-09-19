from __future__ import annotations

"""Canonical A/C long-term watchlists and database synchronizer."""

from dataclasses import dataclass

from .config import settings
from .db import db_conn
from .schema import ensure_schema


@dataclass(frozen=True)
class StrategyCWatchItem:
    symbol: str
    weight: float
    sector: str
    tier: int = 3
    priority: int = 100


STRATEGY_A_WATCHLIST: tuple[StrategyCWatchItem, ...] = (
    StrategyCWatchItem("QQQ", 0.40, "nasdaq_100", 1, 1),
    StrategyCWatchItem("VOO", 0.40, "sp500", 1, 2),
    StrategyCWatchItem("XLV", 0.20, "healthcare_etf", 1, 3),
)


STRATEGY_C_WATCHLIST: tuple[StrategyCWatchItem, ...] = (
    # 30% ETF foundation. C builds these first before individual stocks.
    StrategyCWatchItem("QQQ", 0.120, "nasdaq_100", 1, 1),
    StrategyCWatchItem("VOO", 0.120, "sp500", 1, 2),
    StrategyCWatchItem("XLV", 0.060, "healthcare_etf", 1, 3),
    # 35.7% high-quality core leaders.
    StrategyCWatchItem("BRK.B", 0.056, "financial", 2, 10),
    StrategyCWatchItem("MSFT", 0.049, "ai_platform", 2, 11),
    StrategyCWatchItem("GOOGL", 0.035, "ai_platform", 2, 12),
    StrategyCWatchItem("TSM", 0.028, "foundry", 2, 13),
    StrategyCWatchItem("ASML", 0.028, "lithography", 2, 14),
    StrategyCWatchItem("COST", 0.035, "consumer", 2, 15),
    StrategyCWatchItem("ETN", 0.035, "power", 2, 16),
    StrategyCWatchItem("TMO", 0.035, "life_science", 2, 17),
    StrategyCWatchItem("V", 0.028, "payments", 2, 18),
    StrategyCWatchItem("LIN", 0.028, "industrial_gas", 2, 19),
    # Remaining growth and diversifiers are filled after the foundation/core sleeves.
    StrategyCWatchItem("AMZN", 0.028, "ai_platform", 3, 30),
    StrategyCWatchItem("META", 0.021, "ai_platform", 3, 31),
    StrategyCWatchItem("PANW", 0.014, "cybersecurity", 3, 32),
    StrategyCWatchItem("NVDA", 0.028, "ai_chip", 3, 33),
    StrategyCWatchItem("AVGO", 0.028, "ai_chip", 3, 34),
    StrategyCWatchItem("MU", 0.021, "memory", 3, 35),
    StrategyCWatchItem("ISRG", 0.028, "robotics", 3, 36),
    StrategyCWatchItem("TER", 0.014, "automation", 3, 37),
    StrategyCWatchItem("LLY", 0.028, "healthcare", 3, 38),
    StrategyCWatchItem("VRTX", 0.028, "biotech", 3, 39),
    StrategyCWatchItem("CME", 0.021, "exchange", 3, 40),
    StrategyCWatchItem("CEG", 0.021, "nuclear_power", 3, 41),
    StrategyCWatchItem("GEV", 0.021, "grid_equipment", 3, 42),
    StrategyCWatchItem("SPCX", 0.014, "space", 3, 43),
    StrategyCWatchItem("WM", 0.028, "defensive", 3, 44),
)


def validate_strategy_c_watchlist() -> None:
    symbols = [item.symbol for item in STRATEGY_C_WATCHLIST]
    if len(symbols) != 28:
        raise ValueError(f"Strategy C watchlist must contain 28 symbols, got {len(symbols)}")
    if len(set(symbols)) != len(symbols):
        raise ValueError("Strategy C watchlist contains duplicate symbols")
    total_weight = sum(item.weight for item in STRATEGY_C_WATCHLIST)
    if abs(total_weight - 1.0) > 1e-9:
        raise ValueError(f"Strategy C weights must total 1.0, got {total_weight:.8f}")

    a_symbols = [item.symbol for item in STRATEGY_A_WATCHLIST]
    if a_symbols != ["QQQ", "VOO", "XLV"]:
        raise ValueError(f"Strategy A watchlist is unexpected: {a_symbols}")
    if abs(sum(item.weight for item in STRATEGY_A_WATCHLIST) - 1.0) > 1e-9:
        raise ValueError("Strategy A weights must total 1.0")


def sync_strategy_c_watchlist(*, dry_run: bool = False, prune_legacy: bool = True) -> dict:
    """Replace A/C candidate pools while protecting active broker holdings."""
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
        "a_inserted": 0,
        "a_updated": 0,
        "a_deleted_operations": 0,
        "a_symbols": [],
    }
    with db_conn() as conn:
        with conn.cursor() as cur:
            for item in STRATEGY_A_WATCHLIST:
                cur.execute(
                    f"""
                    SELECT id, is_bought, qty
                    FROM `{table}`
                    WHERE UPPER(stock_code)=%s AND UPPER(stock_type)='A'
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (item.symbol,),
                )
                existing = cur.fetchone()
                action = "updated" if existing else "inserted"
                stats["a_symbols"].append(
                    {
                        "symbol": item.symbol,
                        "weight": item.weight,
                        "sector": item.sector,
                        "action": action,
                    }
                )
                if dry_run:
                    stats[f"a_{action}"] += 1
                    continue

                note = f"A:CORE {item.sector} weight={item.weight:.2%}"[:80]
                if existing:
                    cur.execute(
                        f"""
                        UPDATE `{table}`
                        SET stock_type='A', strategy_group='A', capital_pool='A',
                            weight=%s, margin_used=0,
                            ac_t_enabled=CASE
                                WHEN COALESCE(is_bought,0)=1 OR ABS(COALESCE(qty,0))>0 THEN 1 ELSE 0 END,
                            ac_t_type='A',
                            can_buy=CASE WHEN COALESCE(is_bought,0)=0 THEN 1 ELSE can_buy END,
                            can_sell=CASE WHEN COALESCE(is_bought,0)=0 THEN 0 ELSE can_sell END,
                            last_order_intent=CASE
                                WHEN COALESCE(is_bought,0)=0
                                 AND (last_order_id IS NULL OR last_order_id='')
                                THEN %s ELSE last_order_intent END,
                            updated_at=CURRENT_TIMESTAMP
                        WHERE id=%s
                        """,
                        (item.weight, note, existing["id"]),
                    )
                    stats["a_updated"] += 1
                else:
                    cur.execute(
                        f"""
                        INSERT INTO `{table}` (
                            stock_code, stock_type, weight,
                            is_bought, can_buy, can_sell, qty,
                            strategy_group, capital_pool, margin_used,
                            ac_t_enabled, ac_t_type, ac_t_state,
                            last_order_intent, created_at, updated_at
                        ) VALUES (
                            %s, 'A', %s, 0, 1, 0, 0,
                            'A', 'A', 0, 0, 'A', 'IDLE',
                            %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                        )
                        """,
                        (item.symbol, item.weight, note),
                    )
                    stats["a_inserted"] += 1

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
                            ac_t_enabled=CASE
                                WHEN COALESCE(is_bought,0)=1 OR ABS(COALESCE(qty,0))>0 THEN 1 ELSE 0 END,
                            ac_t_type='C',
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
                        0, 'C', 'IDLE',
                        %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    )
                    """,
                    (item.symbol, item.weight, note),
                )
                stats["inserted"] += 1

            if prune_legacy:
                a_symbols = tuple(item.symbol for item in STRATEGY_A_WATCHLIST)
                a_placeholders = ", ".join(["%s"] * len(a_symbols))
                cur.execute(
                    f"""
                    SELECT id
                    FROM `{table}`
                    WHERE UPPER(COALESCE(NULLIF(strategy_group,''), stock_type))='A'
                      AND UPPER(stock_code) NOT IN ({a_placeholders})
                      AND COALESCE(is_bought,0)=0
                      AND ABS(COALESCE(qty,0))=0
                    """,
                    a_symbols,
                )
                stale_a_ids = [int(row["id"]) for row in cur.fetchall() or []]
                stats["a_deleted_operations"] = len(stale_a_ids)
                if stale_a_ids and not dry_run:
                    delete_placeholders = ", ".join(["%s"] * len(stale_a_ids))
                    cur.execute(
                        f"DELETE FROM `{table}` WHERE id IN ({delete_placeholders})",
                        tuple(stale_a_ids),
                    )

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
                    SELECT id, UPPER(symbol) AS symbol, status,
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
                    if str(row.get("status") or "").lower() == "closed":
                        continue
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
    stats["a_count"] = len(STRATEGY_A_WATCHLIST)
    stats["a_weight_total"] = sum(item.weight for item in STRATEGY_A_WATCHLIST)
    stats["dry_run"] = dry_run
    stats["prune_legacy"] = prune_legacy
    return stats
