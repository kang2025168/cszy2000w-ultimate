from __future__ import annotations

import unittest


class FakeCursor:
    rowcount = 1

    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, args=()):
        self.conn.executed.append((str(sql), args))

    def fetchall(self):
        return []


class FakeConn:
    def __init__(self):
        self.executed = []

    def cursor(self, *_args, **_kwargs):
        return FakeCursor(self)


class QuickTradeTests(unittest.TestCase):
    def test_option_symbol_is_rejected(self):
        import app.quick_trade as qt

        self.assertFalse(qt.is_equity_symbol("MX260918C00007500"))
        self.assertTrue(qt.is_equity_symbol("MX"))

    def test_rank_prefers_strong_tight_spread_candidate(self):
        import app.quick_trade as qt

        rows = [
            {"id": 1, "stock_code": "AAA", "stock_type": "B", "trigger_price": 10.0},
            {"id": 2, "stock_code": "BBB", "stock_type": "B", "trigger_price": 20.0},
        ]
        quotes = {
            "AAA": qt.QuickQuote("AAA", last=10.50, bid=10.49, ask=10.50, day_open=10.10, day_high=10.52, prev_close=10.00),
            "BBB": qt.QuickQuote("BBB", last=20.10, bid=20.09, ask=20.10, day_open=20.00, day_high=20.11, prev_close=20.00),
        }

        accepted, skipped = qt.rank_entry_candidates(rows, lambda symbol: quotes[symbol])

        self.assertEqual("AAA", accepted[0].symbol)
        self.assertEqual([], skipped)

    def test_wide_spread_is_skipped(self):
        import app.quick_trade as qt

        row = {"id": 1, "stock_code": "WIDE", "stock_type": "B", "trigger_price": 10.0}
        quote = qt.QuickQuote("WIDE", last=10.0, bid=9.80, ask=10.20, day_high=10.0, prev_close=9.90)

        plan = qt.score_entry(row, quote)

        self.assertEqual("SKIP", plan.action)
        self.assertEqual("spread_too_wide", plan.reason)

    def test_buy_plan_uses_protected_limit_and_stop_take_profit(self):
        import app.quick_trade as qt

        row = {"id": 7, "stock_code": "FAST", "stock_type": "B", "trigger_price": 10.0}
        quote = qt.QuickQuote("FAST", last=10.0, bid=9.998, ask=10.005, day_open=9.90, day_high=10.02, prev_close=9.80)

        plan = qt.build_buy_plan(row, quote, buying_power=1000.0)

        self.assertEqual("BUY", plan.action)
        self.assertGreater(plan.qty, 0)
        self.assertLessEqual(plan.limit_price, 10.01)
        self.assertGreater(plan.take_profit_price, plan.limit_price)
        self.assertLess(plan.stop_loss_price, plan.limit_price)

    def test_sell_plan_triggers_take_profit(self):
        import app.quick_trade as qt

        row = {
            "id": 8,
            "stock_code": "FAST",
            "stock_type": "B",
            "qty": 25,
            "cost_price": 10.0,
            "take_profit_price": 10.02,
            "stop_loss_price": 9.98,
        }
        quote = qt.QuickQuote("FAST", last=10.03, bid=10.02, ask=10.04, day_high=10.04, prev_close=10.0)

        plan = qt.build_sell_plan(row, quote)

        self.assertEqual("SELL", plan.action)
        self.assertEqual("take_profit", plan.reason)
        self.assertEqual(25, plan.qty)

    def test_execute_plan_dry_run_records_event_without_real_order(self):
        import app.quick_trade as qt

        conn = FakeConn()
        plan = qt.QuickPlan(
            action="BUY",
            symbol="FAST",
            stock_type="B",
            score=12.3,
            reason="strong_tight_spread",
            qty=10,
            limit_price=10.01,
            last=10.0,
            bid=9.99,
            ask=10.01,
            operation_id=1,
        )

        result = qt.execute_plan(conn, plan, dry_run=True)

        self.assertEqual("DRY_RUN", result["status"])
        self.assertTrue(any("quick_trade_events" in sql for sql, _args in conn.executed))


if __name__ == "__main__":
    unittest.main()
