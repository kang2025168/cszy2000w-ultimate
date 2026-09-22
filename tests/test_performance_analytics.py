import unittest

from ultimate_v1.performance_analytics import equity_metrics, strategy_metrics


class PerformanceAnalyticsTests(unittest.TestCase):
    def test_equity_metrics_calculates_peak_drawdown(self) -> None:
        result = equity_metrics([
            {"snapshot_date": "2026-01-01", "equity": 100.0},
            {"snapshot_date": "2026-01-02", "equity": 110.0},
            {"snapshot_date": "2026-01-03", "equity": 93.5},
            {"snapshot_date": "2026-01-04", "equity": 105.0},
        ])
        self.assertEqual(0.05, result["total_return"])
        self.assertEqual(-0.15, result["max_drawdown"])
        self.assertEqual(4, result["sample_days"])

    def test_equity_metrics_resets_after_large_account_change(self) -> None:
        result = equity_metrics([
            {"snapshot_date": "2026-01-01", "equity": 100.0},
            {"snapshot_date": "2026-01-02", "equity": 200.0},
            {"snapshot_date": "2026-01-03", "equity": 190.0},
        ])
        self.assertEqual(2, result["sample_days"])
        self.assertEqual(1, result["account_resets"])
        self.assertEqual(-0.05, result["total_return"])

    def test_strategy_metrics_only_scores_closed_realized_rows(self) -> None:
        rows = [
            {"strategy_group": "B", "status": "closed", "realized_pnl": 100},
            {"strategy_group": "B", "status": "closed", "realized_pnl": -50},
            {"strategy_group": "B", "status": "open", "cost_basis": 1000, "market_value": 1050, "unrealized_pnl": 50},
        ]
        result = next(row for row in strategy_metrics(rows) if row["strategy"] == "B")
        self.assertEqual(2, result["closed_trades"])
        self.assertEqual(0.5, result["win_rate"])
        self.assertEqual(2.0, result["payoff_ratio"])
        self.assertEqual(25.0, result["expectancy"])
        self.assertEqual(1, result["open_positions"])


if __name__ == "__main__":
    unittest.main()
