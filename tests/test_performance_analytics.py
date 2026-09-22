from __future__ import annotations

import unittest

from ultimate_v1.performance_analytics import _closed_trade_summary


class PerformanceAnalyticsTests(unittest.TestCase):
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
