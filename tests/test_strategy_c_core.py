from __future__ import annotations

import unittest

from ultimate_v1.strategy_c_core import build_c_core_buy_plan


class StrategyCCorePlanTests(unittest.TestCase):
    def test_empty_portfolio_builds_etf_foundation_first(self):
        plans = build_c_core_buy_plan(
            target_capital=1500,
            available_capital=1000,
            buying_power=1000,
            cash=1000,
            current_values={},
            daily_budget_pct=0.10,
            daily_budget_max=250,
            min_order=25,
        )

        self.assertEqual(["QQQ", "VOO", "XLV"], [plan.symbol for plan in plans])
        self.assertEqual([60.0, 60.0, 30.0], [plan.notional for plan in plans])

    def test_core_leaders_follow_completed_etf_foundation(self):
        plans = build_c_core_buy_plan(
            target_capital=1000,
            available_capital=1000,
            buying_power=1000,
            cash=1000,
            current_values={"QQQ": 120, "VOO": 120, "XLV": 60},
            daily_budget_pct=0.10,
            daily_budget_max=250,
            min_order=25,
        )

        self.assertEqual(["BRK.B", "MSFT", "GOOGL"], [plan.symbol for plan in plans])
        self.assertEqual(2, plans[0].tier)

    def test_daily_cap_and_cash_reserve_can_block_orders(self):
        plans = build_c_core_buy_plan(
            target_capital=1000,
            available_capital=1000,
            buying_power=1000,
            cash=40,
            current_values={},
            daily_spent=90,
            daily_budget_pct=0.10,
            cash_reserve=25,
            min_order=25,
        )
        self.assertEqual([], plans)

    def test_busy_symbol_is_excluded(self):
        plans = build_c_core_buy_plan(
            target_capital=1500,
            available_capital=1000,
            buying_power=1000,
            cash=1000,
            current_values={},
            excluded_symbols={"QQQ"},
            daily_budget_pct=0.10,
            min_order=25,
        )
        self.assertNotIn("QQQ", [plan.symbol for plan in plans])

class _CoreQtyCursor:
    def __init__(self, allocated_qty):
        self.allocated_qty = allocated_qty
        self.rowcount = 0
        self.last_sql = ""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, args=()):
        self.last_sql = sql
        self.rowcount = 1

    def fetchone(self):
        if "SUM(qty)" in self.last_sql:
            return {"qty": self.allocated_qty}
        return {}


class _CoreQtyConnection:
    def __init__(self, allocated_qty):
        self.cursor_value = _CoreQtyCursor(allocated_qty)

    def cursor(self):
        return self.cursor_value


class StrategyCCoreQuantityTests(unittest.TestCase):
    def test_c_core_uses_group_allocation_not_broker_total(self):
        from app.strategy_ac_t import _core_qty

        row = {
            "id": 1,
            "stock_code": "SMR",
            "stock_type": "C",
            "ac_t_type": "C",
            "ac_t_state": "IDLE",
            "ac_t_core_qty": 466,
            "qty": 125,
        }
        core = _core_qty(_CoreQtyConnection(125), row, real_qty=466)
        self.assertEqual(125, core)
        self.assertEqual(125, row["ac_t_core_qty"])


if __name__ == "__main__":
    unittest.main()
