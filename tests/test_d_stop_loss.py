import unittest
from datetime import datetime,timedelta,timezone
from unittest.mock import patch,MagicMock
from types import SimpleNamespace
from ultimate_v1 import d_grid as d
from ultimate_v1.alpaca_gateway import StockQuote

class StopLossTests(unittest.TestCase):
    def test_boundary_and_freshness(self):
        now=datetime.now(timezone.utc)
        for price,seconds,hit in [(95.01,0,False),(95,0,True),(94,0,True),(94,61,False)]:
            self.assertEqual(hit,d._stop_threshold_hit(100,StockQuote('TEST',price,price,price,now-timedelta(seconds=seconds)),now))
        self.assertFalse(d._stop_threshold_hit(100,StockQuote('TEST',90,90,90),now))

    def test_trigger_latches_and_reuses_cancel_confirm_market_exit(self):
        now=datetime.now(timezone.utc)
        cycle={'symbol':'TEST','state':'SELL_WORKING','cycle_no':1,'buy_order_id':'buy1','buy_filled_price':100,'buy_filled_qty':10}
        client=MagicMock();client.get_clock.return_value=SimpleNamespace(is_open=True)
        with patch.object(d,'_stop_pending',return_value=False),patch.object(d,'get_latest_stock_quote',return_value=StockQuote('TEST',94,94,94,now)),patch.object(d,'_cycle_sell_totals',return_value=(0,0)),patch.object(d,'set_app_setting') as save,patch.object(d,'_event'),patch.object(d,'_last_minute_exit',return_value='cancel_pending') as exit:
            self.assertEqual('cancel_pending',d._handle_stop_loss(MagicMock(),{'symbol':'TEST'},cycle,client,now))
            save.assert_called_once_with('D_STOP_LOSS:TEST','buy1')
            exit.assert_called_once()

    def test_latched_stop_continues_without_new_price(self):
        with patch.object(d,'_stop_pending',return_value=True),patch.object(d,'get_latest_stock_quote') as quote,patch.object(d,'_last_minute_exit',return_value='market_pending'):
            client=MagicMock();client.get_clock.return_value=SimpleNamespace(is_open=True)
            self.assertEqual('market_pending',d._handle_stop_loss(MagicMock(),{}, {},client,datetime.now(timezone.utc)))
            quote.assert_not_called()

    def test_new_buy_does_not_inherit_previous_stop(self):
        with patch.object(d,'get_app_setting',return_value='old-buy'):
            self.assertFalse(d._stop_pending({'symbol':'TEST','buy_order_id':'new-buy'}))
