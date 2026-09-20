import unittest

from ultimate_v1.alpaca_gateway import StockQuote
from ultimate_v1.d_grid import _parse_time_value, _valid_quote, build_grid_plan, sell_limit_from_fill


class DGridTests(unittest.TestCase):
    def test_grid_plan_uses_one_completed_lot(self) -> None:
        plan = build_grid_plan(10.00, 250.00, 0.03, 0.06)
        self.assertEqual(10.00, plan.anchor_price)
        self.assertEqual(9.97, plan.buy_limit)
        self.assertEqual(25, plan.qty)
        self.assertEqual(249.25, plan.notional)
        self.assertEqual(10.03, plan.sell_limit)

    def test_sell_target_is_based_on_actual_fill(self) -> None:
        self.assertEqual(10.02, sell_limit_from_fill(9.96, 0.06))

    def test_percentage_grid_scales_with_share_price(self) -> None:
        plan = build_grid_plan(100.00, 1_000.00, entry_pct=0.0025, profit_pct=0.01)
        self.assertEqual(100.00, plan.anchor_price)
        self.assertEqual(99.75, plan.buy_limit)
        self.assertEqual(10, plan.qty)
        self.assertEqual(997.50, plan.notional)
        self.assertEqual(100.75, plan.sell_limit)

        high_price_plan = build_grid_plan(1_000.00, 10_000.00, entry_pct=0.0025, profit_pct=0.01)
        self.assertEqual(997.50, high_price_plan.buy_limit)
        self.assertEqual(1_007.48, high_price_plan.sell_limit)

    def test_percentage_sell_target_uses_actual_fill(self) -> None:
        self.assertEqual(100.60, sell_limit_from_fill(99.60, profit_pct=0.01))

    def test_quote_filter_does_not_require_bid_ask_spread(self) -> None:
        valid, reason, anchor = _valid_quote(StockQuote("TEST", 10.02, 10.00, 10.10), 0.05)
        self.assertTrue(valid)
        self.assertEqual("ok", reason)
        self.assertEqual(10.02, anchor)

    def test_quote_filter_prefers_last_inside_spread(self) -> None:
        valid, reason, anchor = _valid_quote(StockQuote("TEST", 10.02, 10.00, 10.04), 0.05)
        self.assertTrue(valid)
        self.assertEqual("ok", reason)
        self.assertEqual(10.02, anchor)

    def test_runtime_time_parser_rejects_invalid_time(self) -> None:
        self.assertEqual(12, _parse_time_value("12:30").hour)
        with self.assertRaises(ValueError):
            _parse_time_value("25:90")
