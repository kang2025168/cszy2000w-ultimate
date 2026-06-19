from __future__ import annotations

import unittest
from datetime import datetime, timedelta


class FakeConn:
    def close(self):
        pass


class StrategyBSellStopGraceTests(unittest.TestCase):
    def _run_case(self, price: float):
        import app.strategy_b as b

        calls = []
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        row = {
            "stock_code": "MOCKB",
            "stock_type": "B",
            "is_bought": 1,
            "can_sell": 1,
            "qty": 10,
            "cost_price": 100.0,
            "stop_loss_price": 99.0,
            "trigger_price": 100.0,
            "b_stage": 0,
            "last_order_side": "buy",
            "last_order_time": now_str,
        }
        originals = {
            "_connect": b._connect,
            "_load_one_b_row": b._load_one_b_row,
            "get_snapshot_realtime": b.get_snapshot_realtime,
            "_update_ops_fields": b._update_ops_fields,
            "_sell_qty": b._sell_qty,
        }
        try:
            b._connect = lambda: FakeConn()
            b._load_one_b_row = lambda _conn, _code: dict(row)
            b.get_snapshot_realtime = lambda _code: (price, 100.0, "test")
            b._update_ops_fields = lambda *_args, **_kwargs: None
            b._sell_qty = lambda _conn, code, qty, reason: calls.append((code, qty, reason)) or True
            result = b.strategy_B_sell("MOCKB")
            return result, calls
        finally:
            b._connect = originals["_connect"]
            b._load_one_b_row = originals["_load_one_b_row"]
            b.get_snapshot_realtime = originals["get_snapshot_realtime"]
            b._update_ops_fields = originals["_update_ops_fields"]
            b._sell_qty = originals["_sell_qty"]

    def test_initial_stop_grace_blocks_normal_stop(self):
        result, calls = self._run_case(98.0)

        self.assertFalse(result)
        self.assertEqual([], calls)

    def test_initial_stop_grace_allows_catastrophic_stop(self):
        result, calls = self._run_case(94.0)

        self.assertTrue(result)
        self.assertEqual(1, len(calls))
        self.assertIn("STOP", calls[0][2])


if __name__ == "__main__":
    unittest.main()
