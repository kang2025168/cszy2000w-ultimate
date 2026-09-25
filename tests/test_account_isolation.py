from __future__ import annotations

import unittest

from ultimate_v1.sync_positions import _resolve_strategy_group
from ultimate_v1.web_app import _pool_account_buying_power


class _Cursor:
    def __init__(self, result_sets):
        self._result_sets = result_sets
        self._rows = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, _sql, _args):
        self._rows = self._result_sets.pop(0)

    def fetchone(self):
        return self._rows.pop(0) if self._rows else None

    def fetchall(self):
        return self._rows


class _Connection:
    def __init__(self, result_sets):
        self.result_sets = result_sets

    def cursor(self):
        return _Cursor(self.result_sets)


class AccountIsolationTests(unittest.TestCase):
    def test_group_resolution_respects_broker_account_scope(self):
        rows = [
            {"stock_type": "A", "strategy_group": "A"},
            {"stock_type": "C", "strategy_group": "C"},
        ]
        # First query checks for an open D holding; none exists here.
        conn = _Connection([[], rows])

        self.assertEqual(
            "C",
            _resolve_strategy_group(
                conn,
                "stock_operations",
                "QQQ",
                allowed_groups={"B", "C", "D", "F"},
                default_group="B",
            ),
        )

    def test_group_resolution_keeps_retirement_position_in_a(self):
        rows = [
            {"stock_type": "A", "strategy_group": "A"},
            {"stock_type": "C", "strategy_group": "C"},
        ]
        conn = _Connection([rows])

        self.assertEqual(
            "A",
            _resolve_strategy_group(
                conn,
                "stock_operations",
                "QQQ",
                allowed_groups={"A"},
                default_group="A",
            ),
        )

    def test_open_d_holding_takes_priority_in_trading_account(self):
        conn = _Connection([[{"id": 42}]])
        self.assertEqual("D", _resolve_strategy_group(
            conn, "stock_operations", "META",
            allowed_groups={"B", "C", "D", "F"}, default_group="B"))
        self.assertEqual([], conn.result_sets)

    def test_missing_d_and_ops_falls_back_to_scoped_holdings(self):
        conn = _Connection([[], [], [
            {"stock_type": "A", "strategy_group": "A"},
            {"stock_type": "C", "strategy_group": "C"},
        ]])
        self.assertEqual("C", _resolve_strategy_group(
            conn, "stock_operations", "QQQ",
            allowed_groups={"B", "C", "D", "F"}, default_group="B"))

    def test_manual_order_uses_selected_pool_broker_buying_power(self):
        capital = {
            "buying_power": 99_999,
            "pool_brokers": {"A": "retirement", "B": "trading", "C": "trading", "D": "trading"},
            "broker_snapshots": {
                "retirement": {"buying_power": 125},
                "trading": {"buying_power": 4_000},
            },
        }

        self.assertEqual(125, _pool_account_buying_power(capital, "A"))
        self.assertEqual(4_000, _pool_account_buying_power(capital, "C"))
        self.assertEqual(0, _pool_account_buying_power(capital, "UNKNOWN"))
