import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from ultimate_v1 import d_grid as d


class CloseoutRepairTests(unittest.TestCase):
    def test_closeout_uses_current_price_limit_not_profit_target(self):
        from ultimate_v1.alpaca_gateway import StockQuote
        cur,client=MagicMock(),MagicMock()
        client.get_clock.return_value=SimpleNamespace(is_open=True)
        cycle={'symbol':'META','cycle_no':6,'buy_order_id':'buy','sell_order_id':'old','buy_filled_qty':1}
        with patch.object(d,'_set_cycle'),patch.object(d,'_event'),patch.object(d,'_submit_limit',return_value=SimpleNamespace(id='new')) as submit:
            d._submit_sell(cur,{'symbol':'META','profit_offset':.06},cycle,1,777.46,False,client,closing=True,quote=StockQuote('META',777.1,0,0))
            self.assertEqual(777.1,submit.call_args.args[4])
            self.assertIn('-cl-',submit.call_args.args[5])

    def test_missing_current_price_does_not_use_profit_target(self):
        from ultimate_v1.alpaca_gateway import StockQuote
        client=MagicMock()
        client.get_clock.return_value=SimpleNamespace(is_open=True)
        with patch.object(d,'_submit_limit') as submit:
            result=d._submit_sell(MagicMock(),{'symbol':'META'}, {'cycle_no':1},1,777.46,False,client,closing=True,quote=StockQuote('META',0,0,0))
        self.assertEqual('close_waiting_current_price',result)
        submit.assert_not_called()

    def test_partial_fills_across_orders_are_combined(self):
        cur=MagicMock()
        cur.fetchall.return_value=[{'order_id':'old'},{'order_id':'new'}]
        client=MagicMock()
        client.get_order_by_id.side_effect=lambda oid: SimpleNamespace(status='filled',filled_qty=4 if oid=='old' else 6,filled_avg_price=110 if oid=='old' else 100)
        qty,value=d._cycle_sell_totals(cur,{'symbol':'TEST','cycle_no':1,'sell_order_id':'new'},client)
        self.assertEqual((10,1040),(qty,value))

    def test_expired_close_retries_only_remaining_quantity(self):
        cur,client=MagicMock(),MagicMock()
        client.get_order_by_id.return_value=SimpleNamespace(status='expired',filled_qty=2,filled_avg_price=100)
        client.get_clock.return_value=SimpleNamespace(is_open=True)
        cycle={'symbol':'TEST','cycle_no':1,'sell_order_id':'old','buy_filled_qty':10,'buy_filled_price':99}
        with patch.object(d,'_cycle_sell_totals',return_value=(4,400)),patch.object(d,'_submit_sell',return_value='retry') as submit:
            self.assertEqual('retry',d._advance_sell(cur,{'symbol':'TEST'},cycle,None,False,client,closing=True))
            self.assertEqual(6,submit.call_args.args[3])
            self.assertTrue(submit.call_args.kwargs['closing'])

    def test_full_fill_uses_weighted_sale_price(self):
        client=MagicMock()
        client.get_order_by_id.return_value=SimpleNamespace(status='filled',filled_qty=6,filled_avg_price=100)
        with patch.object(d,'_cycle_sell_totals',return_value=(10,1040)),patch.object(d,'_finish_cycle') as finish:
            d._advance_sell(MagicMock(),{}, {'sell_order_id':'new','buy_filled_qty':10},None,False,client,closing=True)
            self.assertEqual(104,finish.call_args.args[3])
