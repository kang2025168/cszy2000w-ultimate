from __future__ import annotations

import unittest

from ultimate_v1.strategy_c_watchlist import (
    STRATEGY_A_WATCHLIST,
    STRATEGY_C_WATCHLIST,
    validate_strategy_c_watchlist,
)


class StrategyCWatchlistTests(unittest.TestCase):
    def test_watchlists_have_expected_symbols_and_full_weight(self):
        validate_strategy_c_watchlist()

        symbols = [item.symbol for item in STRATEGY_C_WATCHLIST]
        self.assertEqual(30, len(symbols))
        self.assertEqual(30, len(set(symbols)))
        self.assertAlmostEqual(1.0, sum(item.weight for item in STRATEGY_C_WATCHLIST))
        self.assertEqual(["QQQ", "VOO", "XLV"], [item.symbol for item in STRATEGY_A_WATCHLIST])
        self.assertAlmostEqual(1.0, sum(item.weight for item in STRATEGY_A_WATCHLIST))

    def test_expected_core_symbols_are_present(self):
        weights = {item.symbol: item.weight for item in STRATEGY_C_WATCHLIST}

        self.assertEqual(0.10, weights["QQQ"])
        self.assertEqual(0.09, weights["VOO"])
        self.assertEqual(0.06, weights["XLV"])
        self.assertEqual(0.03, weights["IAU"])
        self.assertEqual(0.02, weights["IBIT"])
        self.assertEqual(0.049, weights["MSFT"])
        self.assertEqual(0.056, weights["BRK.B"])
        self.assertEqual(0.014, weights["SPCX"])


if __name__ == "__main__":
    unittest.main()
