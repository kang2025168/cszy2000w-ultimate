from __future__ import annotations

import unittest

from ultimate_v1.strategy_c_watchlist import STRATEGY_C_WATCHLIST, validate_strategy_c_watchlist


class StrategyCWatchlistTests(unittest.TestCase):
    def test_watchlist_has_25_unique_symbols_and_full_weight(self):
        validate_strategy_c_watchlist()

        symbols = [item.symbol for item in STRATEGY_C_WATCHLIST]
        self.assertEqual(25, len(symbols))
        self.assertEqual(25, len(set(symbols)))
        self.assertAlmostEqual(1.0, sum(item.weight for item in STRATEGY_C_WATCHLIST))

    def test_expected_core_symbols_are_present(self):
        weights = {item.symbol: item.weight for item in STRATEGY_C_WATCHLIST}

        self.assertEqual(0.07, weights["MSFT"])
        self.assertEqual(0.08, weights["BRK.B"])
        self.assertEqual(0.02, weights["SPCX"])


if __name__ == "__main__":
    unittest.main()

