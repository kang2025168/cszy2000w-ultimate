import json
import unittest
from unittest.mock import MagicMock, patch
from ultimate_v1 import daily_pnl as d


def position(symbol='TEST', qty=10, price=90):
    return {'symbol':symbol, 'qty':qty, 'current_price':price,
            'market_value':qty*price, 'unrealized_pl':-50}


def fill(side, qty, price, symbol='TEST'):
    return {'symbol':symbol, 'side':side, 'qty':qty, 'price':price}


class DailyPnlTests(unittest.TestCase):
    def test_holding_daily_loss_is_not_lifetime_loss(self):
        r = d.attribute([position()], [], {'TEST':100})[0]
        self.assertEqual(-100, r['daily_pnl'])
        self.assertEqual(-50, r['unrealized_pnl'])

    def test_intraday_roundtrip_and_remaining_position(self):
        r = d.attribute([position(qty=1, price=777.07)],
                        [fill('buy',1,764.87),fill('sell',1,772.52),
                         fill('buy',1,769.37),fill('sell',1,777.06),
                         fill('buy',1,777.46)], {'TEST':750})[0]
        self.assertEqual(14.95, r['daily_pnl'])

    def test_overnight_partial_sell_marks_both_parts(self):
        r = d.attribute([position(qty=6,price=110)], [fill('sell',4,105)], {'TEST':100})[0]
        self.assertEqual(80, r['daily_pnl'])

    def test_missing_quote_and_options_are_not_zero_profit(self):
        self.assertIsNone(d.attribute([position()],[],{})[0]['daily_pnl'])
        symbol='AAPL260925C00200000'
        self.assertIsNone(d.attribute([], [fill('buy',1,2,symbol),fill('sell',1,3,symbol)],{})[0]['daily_pnl'])
        self.assertEqual(10,d.attribute([], [fill('buy',1,100),fill('sell',1,110)],{})[0]['daily_pnl'])

    def test_history_reads_saved_report_without_broker_recalculation(self):
        report={'date':'2026-09-23','equity':3000}
        with patch.object(d,'fetch_all',side_effect=[[{'report_date':'2026-09-23'}],[{'payload':json.dumps(report)}]]), patch.object(d,'collect_report') as collect:
            result=d.report_payload('2026-09-23')
        self.assertEqual(report,result['report'])
        collect.assert_not_called()

    def test_save_is_keyed_by_day_without_deleting_history(self):
        conn=MagicMock()
        with patch.object(d,'db_conn',return_value=conn):
            d.save_report({'date':'2026-09-23'})
            d.save_report({'date':'2026-09-24'})
        calls=conn.__enter__.return_value.cursor.return_value.__enter__.return_value.execute.call_args_list
        self.assertEqual(['2026-09-23','2026-09-24'],[c.args[1][0] for c in calls])
        self.assertTrue(all('ON DUPLICATE KEY UPDATE' in c.args[0] for c in calls))

    def test_invalid_date_rejected(self):
        with self.assertRaises(ValueError):
            d.report_payload('2026-09-24 OR 1=1')
