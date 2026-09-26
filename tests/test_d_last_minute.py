import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from ultimate_v1 import d_grid as d

class LastMinuteTests(unittest.TestCase):
    def run_case(self,status,cid='limit'):
        client=MagicMock()
        client.get_order_by_id.return_value=SimpleNamespace(status=status,filled_qty=2,filled_avg_price=100,client_order_id=cid)
        client.get_open_position.return_value=SimpleNamespace(qty=28)
        cycle={'sell_order_id':'old','buy_filled_qty':10,'buy_filled_price':99}
        with patch.object(d,'_cycle_sell_totals',return_value=(2,200)),patch.object(d,'_submit_sell',return_value='submitted') as submit:
            result=d._last_minute_exit(MagicMock(),{'symbol':'META'},cycle,client)
        return client,submit,result

    def test_active_limit_is_canceled_before_market_submission(self):
        client,submit,_=self.run_case('new')
        client.cancel_order_by_id.assert_called_once_with('old')
        submit.assert_not_called()

    def test_confirmed_cancel_only_sells_remaining(self):
        _,submit,_=self.run_case('canceled')
        self.assertEqual(8,submit.call_args.args[3])
        self.assertTrue(submit.call_args.kwargs['force_market'])

    def test_market_order_is_not_replaced_again(self):
        client,submit,result=self.run_case('new','dgrid-META-6-cm-old')
        client.cancel_order_by_id.assert_not_called()
        submit.assert_not_called()
        self.assertEqual('waiting_market_close_fill',result)

    def test_request_is_market_and_has_no_limit_price(self):
        from alpaca.trading.requests import MarketOrderRequest
        with patch('ultimate_v1.order_journal.get_intent',return_value={}),patch('ultimate_v1.order_journal.submit_prepared') as submit:
            d._submit_limit(MagicMock(),'META','sell',8,785.23,'dgrid-META-6-cm-old')
        request=submit.call_args.args[1]
        self.assertIsInstance(request,MarketOrderRequest)
        self.assertFalse(hasattr(request,'limit_price'))
