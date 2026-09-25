import unittest
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app import strategy_b as b


class PeakGivebackTests(unittest.TestCase):
    def test_thresholds_and_monotonic_tier_boundaries(self):
        self.assertIsNone(b._peak_giveback_trigger(100, 104.99))
        for peak, trigger in ((105, 102.9), (110, 107.8), (115, 111.55), (120, 115.2)):
            self.assertAlmostEqual(trigger, b._peak_giveback_trigger(100, peak))
        values = [b._peak_giveback_trigger(100, p / 100) for p in range(10500, 16001)]
        self.assertEqual(values, sorted(values))

    def test_only_confirmed_full_exit_enters_f_watchlist(self):
        for filled in (0, 4, 10):
            with self.subTest(filled=filled), ExitStack() as stack:
                conn = MagicMock()
                client = MagicMock()
                client.get_order_by_id.return_value = SimpleNamespace(filled_avg_price=102.9, status='filled')
                row = {'cost_price':100, 'b_peak_price':105, 'b_peak_profit':50}
                mocks = {
                    '_get_trading_client': client,
                    '_get_real_position_qty':10,
                    '_load_one_b_row':row,
                    '_submit_limit_sell_qty':SimpleNamespace(id='test',status='new'),
                    '_reconcile_sell_fill':filled,
                }
                for name, value in mocks.items():
                    stack.enter_context(patch.object(b, name, return_value=value))
                stack.enter_context(patch.object(b, '_decision_log'))
                stack.enter_context(patch.object(b, '_update_ops_fields'))
                watch = stack.enter_context(patch.object(b, '_write_monster_watchlist'))
                result = b._sell_qty(conn, 'TEST', 10, 'PEAK_GIVEBACK test', limit_price=102.9)
                self.assertEqual(bool(filled), result)
                self.assertEqual(int(filled == 10), watch.call_count)
                if filled == 10:
                    self.assertEqual(102.9, watch.call_args.args[3])
                    self.assertEqual(105, watch.call_args.args[4]['b_peak_price'])

    def test_giveback_exit_reactivates_f_observation(self):
        conn = MagicMock()
        with patch.object(b, '_ensure_monster_watchlist_table'), patch.object(b, 'B_MONSTER_MIN_PEAK_GAIN_PCT', 0.2):
            b._write_monster_watchlist(conn, 'TEST', 'PEAK_GIVEBACK test', 102.9,
                                      {'cost_price':100, 'b_peak_price':105})
        sql = conn.cursor.return_value.__enter__.return_value.execute.call_args.args[0]
        self.assertIn("watch_status='WATCHING'", sql)
        self.assertIn('watch_since=NOW()', sql)


if __name__ == '__main__':
    unittest.main()
