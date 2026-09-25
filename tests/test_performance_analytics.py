from __future__ import annotations

import unittest

from ultimate_v1.performance_analytics import _closed_trade_summary


class PerformanceAnalyticsTests(unittest.TestCase):
    def test_d_details_include_buy_event_without_claiming_fill_time(self):
        from unittest.mock import patch
        from ultimate_v1.performance_analytics import _d_closed_trades
        event = dict(created_at='2026-09-24 11:46:45', symbol='META', cycle_no=3,
                     qty=1, price=777.06, message='gross_pnl=7.69', order_id='sell-3',
                     entry_order_id='buy-3', entry_submitted_at='2026-09-24 10:29:18')
        with patch('ultimate_v1.performance_analytics._safe_fetch', return_value=[event]) as fetch:
            row = _d_closed_trades(None)[0]
        self.assertEqual('buy-3', row['entry_order_id'])
        self.assertEqual('sell-3', row['exit_order_id'])
        self.assertEqual(event['entry_submitted_at'], row['entry_submitted_at'])
        self.assertNotIn('started_at', row)
        self.assertAlmostEqual(769.37, row['entry_price'])
        sql = fetch.call_args.args[0]
        self.assertIn('buy.cycle_no=e.cycle_no', sql)
        self.assertIn('buy.id < e.id', sql)

    def test_d_missing_buy_event_keeps_completed_trade(self):
        from unittest.mock import patch
        from ultimate_v1.performance_analytics import _d_closed_trades
        with patch('ultimate_v1.performance_analytics._safe_fetch', return_value=[
            dict(symbol='META', qty=1, price=777.06, message='gross_pnl=7.69')]):
            row = _d_closed_trades(None)[0]
        self.assertIsNone(row['entry_order_id'])
        self.assertEqual(7.69, row['realized_pnl'])

    def test_closed_trade_summary_keeps_all_strategy_rows(self):
        rows = [
            {"strategy_group": "A", "realized_pnl": 5, "cost_effect": "LOWERED"},
            {"strategy_group": "B", "realized_pnl": -2, "cost_effect": "LOSS"},
            {"strategy_group": "D", "realized_pnl": 3, "cost_effect": "PROFIT"},
            {"strategy_group": "Q", "realized_pnl": 4, "cost_effect": "PROFIT"},
        ]

        result = {row["strategy"]: row for row in _closed_trade_summary(rows)}

        self.assertEqual(["A", "B", "C", "D", "Q"], list(result))
        self.assertEqual(1, result["A"]["lowered"])
        self.assertEqual(1, result["B"]["losses"])
        self.assertEqual(1, result["D"]["wins"])
        self.assertEqual(4.0, result["Q"]["realized_pnl"])
        self.assertEqual(0, result["C"]["cycles"])


if __name__ == "__main__":
    unittest.main()
