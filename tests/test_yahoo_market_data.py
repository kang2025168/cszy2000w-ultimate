from __future__ import annotations

import unittest
from unittest.mock import patch

from ultimate_v1 import yahoo_market_data as yahoo
from ultimate_v1.yahoo_market_data import YahooStockQuote


class YahooMarketDataTests(unittest.TestCase):
    def setUp(self):
        yahoo._quote_cache.clear()
        yahoo._quote_error_until.clear()

    def test_class_share_symbol_is_translated_only_for_yahoo(self):
        fetched = YahooStockQuote(symbol="BRK-B", last=500.0, prev_close=495.0)

        with patch.object(yahoo, "_sleep_for_yahoo_rate_limit"), patch.object(
            yahoo, "_chart_quote", return_value=fetched
        ) as chart_quote:
            result = yahoo.get_yahoo_stock_quote("brk.b")

        chart_quote.assert_called_once_with("BRK-B")
        self.assertEqual("BRK.B", result.symbol)
        self.assertEqual(500.0, result.last)
        self.assertIn("BRK.B", yahoo._quote_cache)

    def test_regular_symbol_is_unchanged(self):
        self.assertEqual("MSFT", yahoo._yahoo_symbol("msft"))


if __name__ == "__main__":
    unittest.main()
