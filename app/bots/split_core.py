# -*- coding: utf-8 -*-
"""
Shared runtime for independent buy and sell bots.

The strategy functions still live in their current modules. This layer only
separates the process loops so buying and selling can be operated, scheduled,
and optimized independently.
"""
from __future__ import annotations

import os
import random
import time as t
import traceback
from dataclasses import dataclass

from app import trade_bot_main as tb


DEFAULT_STRATEGIES = ("B", "F")
LOG_EACH_SYMBOL = int(os.getenv("SPLIT_BOT_LOG_EACH_SYMBOL", "1"))
VALID_PHASES = {"premarket_sell", "preopen_record", "regular", "afterhours_add", "closed"}


@dataclass(frozen=True)
class SplitBotConfig:
    role: str
    strategies: tuple[str, ...]
    sleep_between_rounds: float
    round_jitter_max: float


def load_config(role: str) -> SplitBotConfig:
    strategies_raw = os.getenv("BOT_STRATEGIES", ",".join(DEFAULT_STRATEGIES))
    strategies = tuple(
        x.strip().upper()
        for x in strategies_raw.split(",")
        if x.strip()
    ) or DEFAULT_STRATEGIES

    role_prefix = role.upper()
    default_sleep = "5" if role == "sell" else str(tb.SLEEP_BETWEEN_ROUNDS)
    sleep_between_rounds = float(
        os.getenv(f"{role_prefix}_BOT_SLEEP_BETWEEN_ROUNDS", os.getenv("BOT_SLEEP_BETWEEN_ROUNDS", default_sleep))
    )
    round_jitter_max = float(os.getenv("BOT_ROUND_JITTER_MAX", str(tb.ROUND_JITTER_MAX)))

    return SplitBotConfig(
        role=role,
        strategies=strategies,
        sleep_between_rounds=sleep_between_rounds,
        round_jitter_max=round_jitter_max,
    )


def _strategy_enabled(stype: str) -> bool:
    return tb._strategy_buy_enabled(stype)


def _sell_one(code: str, stype: str, phase: str) -> bool:
    if stype == "B":
        if phase == "premarket_sell":
            return tb.safe_call(tb.strategy_B_premarket_manage, code) is True
        if phase == "preopen_record":
            tb.safe_call(tb.strategy_B_extended_record, code, phase)
            return False
        if phase == "afterhours_add":
            return tb.safe_call(tb.strategy_B_afterhours_add, code) is True
        if phase == "regular":
            return tb.safe_call(tb.strategy_B_sell, code) is True

    if stype == "F":
        if phase == "premarket_sell":
            return tb.safe_call(tb.strategy_F_premarket_manage, code) is True
        if phase == "preopen_record":
            tb.safe_call(tb.strategy_F_extended_record, code, phase)
            return False
        if phase == "afterhours_add":
            return tb.safe_call(tb.strategy_F_afterhours_add, code) is True
        if phase == "regular":
            return tb.safe_call(tb.strategy_F_sell, code) is True

    return False


def _buy_one(code: str, stype: str) -> bool:
    if not _strategy_enabled(stype):
        tb.log.info(f"[BUY BOT] skip {code}: strategy_{stype.lower()}_enabled=0")
        return False

    if stype == "B":
        return tb.safe_call(tb.strategy_B_buy, code) is True
    if stype == "F":
        return tb.safe_call(tb.strategy_F_buy, code) is True
    return False


def run_sell_round(conn, config: SplitBotConfig, phase: str) -> tuple[object, bool]:
    conn = tb.ensure_conn_alive(conn)
    traded_any = False
    rows = tb.load_rows(conn, mode="sell") or []
    scanned = 0
    eligible = 0
    tb.log.info(
        f"[SELL BOT] round phase={phase} db_rows={len(rows)} "
        f"strategies={','.join(config.strategies)}"
    )

    for row in rows:
        if tb._STOP:
            break
        scanned += 1

        code = (row.get("stock_code") or "").strip().upper()
        stype = (row.get("stock_type") or "").strip().upper()
        is_bought = int(row.get("is_bought") or 0)
        can_sell = int(row.get("can_sell") or 0)

        if not code or stype not in config.strategies:
            continue
        if is_bought != 1 or can_sell != 1:
            continue

        eligible += 1
        if LOG_EACH_SYMBOL:
            tb.log.info(f"[SELL BOT] scan {stype} {code} phase={phase}")
        traded = _sell_one(code, stype, phase)
        if traded:
            traded_any = True
            t.sleep(float(os.getenv("AFTER_TRADE_SLEEP_SEC", "2")))

        t.sleep(tb.SLEEP_BETWEEN_SYMBOLS + random.uniform(0, 0.08))

    tb.log.info(
        f"[SELL BOT] round done phase={phase} scanned={scanned} "
        f"eligible={eligible} traded={int(traded_any)}"
    )
    return conn, traded_any


def _buy_allowed(conn, phase: str, control: dict) -> bool:
    if phase != "regular":
        tb.log.info(f"[BUY BOT] skipped: phase={phase}")
        return False
    if control.get("sell_only_mode") == 1:
        tb.log.info("[BUY BOT] skipped: sell_only_mode=1")
        return False
    if control.get("global_buy_enabled") != 1:
        tb.log.info("[BUY BOT] skipped: global_buy_enabled=0")
        return False
    if not tb.refresh_buy_gate(force=False):
        tb.log.info("[BUY BOT] skipped: buying power gate closed")
        return False
    if tb.get_market_gate(conn) != 1:
        tb.log.info("[BUY BOT] skipped: market gate closed")
        return False
    return True


def run_buy_round(conn, config: SplitBotConfig, phase: str, control: dict) -> tuple[object, bool]:
    conn = tb.ensure_conn_alive(conn)
    traded_any = False

    if not _buy_allowed(conn, phase, control):
        return conn, traded_any

    if "F" in config.strategies:
        tb.safe_call(tb.strategy_F_refresh_candidates)

    rows = tb.load_rows(conn, mode="buy") or []
    scanned = 0
    eligible = 0
    b_codes = []
    other_rows = []
    tb.log.info(
        f"[BUY BOT] round phase={phase} db_rows={len(rows)} "
        f"strategies={','.join(config.strategies)}"
    )

    for row in rows:
        if tb._STOP:
            break
        scanned += 1

        code = (row.get("stock_code") or "").strip().upper()
        stype = (row.get("stock_type") or "").strip().upper()
        can_buy = int(row.get("can_buy") or 0)

        if not code or stype not in config.strategies:
            continue
        if can_buy != 1:
            continue

        eligible += 1
        if stype == "B":
            b_codes.append(code)
            continue
        other_rows.append((code, stype))

    if b_codes:
        confirmed_b = tb.safe_call(tb.strategy_B_rank_and_confirm, b_codes) or []
        for code in confirmed_b:
            if tb._STOP:
                break
            if LOG_EACH_SYMBOL:
                tb.log.info(f"[BUY BOT] confirmed B {code} phase={phase}")
            traded = _buy_one(code, "B")
            if traded:
                traded_any = True
                t.sleep(float(os.getenv("AFTER_TRADE_SLEEP_SEC", "2")))
                tb.refresh_buy_gate(force=True)

            t.sleep(tb.SLEEP_BETWEEN_SYMBOLS + random.uniform(0, 0.08))

    for code, stype in other_rows:
        if tb._STOP:
            break

        if LOG_EACH_SYMBOL:
            tb.log.info(f"[BUY BOT] scan {stype} {code} phase={phase}")
        traded = _buy_one(code, stype)
        if traded:
            traded_any = True
            t.sleep(float(os.getenv("AFTER_TRADE_SLEEP_SEC", "2")))
            tb.refresh_buy_gate(force=True)

        t.sleep(tb.SLEEP_BETWEEN_SYMBOLS + random.uniform(0, 0.08))

    tb.log.info(
        f"[BUY BOT] round done phase={phase} scanned={scanned} "
        f"eligible={eligible} traded={int(traded_any)}"
    )
    return conn, traded_any


def main_loop(role: str) -> None:
    config = load_config(role)
    tb.log.info(
        f"===== split {role} bot start ===== env={tb.TRADE_ENV} "
        f"strategies={','.join(config.strategies)}"
    )

    conn = None
    round_no = 0
    if role == "buy":
        tb.refresh_buy_gate(force=True)

    while not tb._STOP:
        try:
            round_no += 1
            phase = tb.get_trade_phase()
            forced_phase = (
                os.getenv(f"{role.upper()}_BOT_FORCE_PHASE")
                or os.getenv("SPLIT_BOT_FORCE_PHASE")
                or ""
            ).strip()
            if forced_phase:
                if forced_phase not in VALID_PHASES:
                    tb.log.warning(
                        f"[{role.upper()} BOT] ignore invalid force phase={forced_phase}; "
                        f"valid={','.join(sorted(VALID_PHASES))}"
                    )
                elif tb.TRADE_ENV != "paper" and os.getenv("ALLOW_LIVE_FORCE_PHASE", "0") != "1":
                    tb.log.warning(
                        f"[{role.upper()} BOT] ignore force phase={forced_phase}: "
                        "only paper env is allowed unless ALLOW_LIVE_FORCE_PHASE=1"
                    )
                else:
                    real_phase = phase
                    phase = forced_phase
                    tb.log.warning(
                        f"[{role.upper()} BOT] FORCE phase real={real_phase} effective={phase} "
                        f"env={tb.TRADE_ENV}"
                    )
            os.environ["TRADE_PHASE"] = phase

            # if phase == "closed":
            #     tb.log.info(f"[{role.upper()} BOT] market closed, sleep 60s")
            #     t.sleep(60)
            #     continue

            if conn is None:
                conn = tb.get_conn()
                tb.log.info(f"[{role.upper()} BOT] DB connected")

            conn = tb.ensure_conn_alive(conn)
            control = tb.load_bot_control(conn)
            tb.log.info(
                f"[{role.upper()} BOT] loop round={round_no} phase={phase} "
                f"emergency_stop={control.get('emergency_stop')} "
                f"sell_only={control.get('sell_only_mode')} "
                f"global_buy={control.get('global_buy_enabled')}"
            )

            if control.get("emergency_stop") == 1:
                tb.log.warning(f"[{role.upper()} BOT] emergency_stop=1, pause")
                t.sleep(30)
                continue

            if role == "sell":
                conn, traded_once = run_sell_round(conn, config, phase)
            elif role == "buy":
                conn, traded_once = run_buy_round(conn, config, phase, control)
            else:
                raise RuntimeError(f"unknown split bot role={role}")

            if traded_once:
                t.sleep(float(os.getenv("AFTER_ROUND_TRADE_SLEEP_SEC", "0.5")))
            else:
                t.sleep(config.sleep_between_rounds + random.uniform(0, config.round_jitter_max))

        except Exception as e:
            tb.log.error(f"[{role.upper()} BOT] loop error: {e}")
            traceback.print_exc()
            backoff = random.randint(tb.ERROR_BACKOFF_MIN, tb.ERROR_BACKOFF_MAX)
            tb.log.warning(f"[{role.upper()} BOT] backoff {backoff}s")
            t.sleep(backoff)
            try:
                if conn:
                    conn.close()
            except Exception:
                pass
            conn = None

    try:
        if conn:
            conn.close()
    except Exception:
        pass
    tb.log.info(f"===== split {role} bot stopped =====")
