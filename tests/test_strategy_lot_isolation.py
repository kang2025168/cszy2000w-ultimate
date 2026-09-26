import unittest
from unittest.mock import patch, MagicMock
from types import SimpleNamespace
from ultimate_v1 import d_grid as d
from app import strategy_b as b

class StrategyLotIsolationTests(unittest.TestCase):
    def test_b_cancel_never_cancels_d_order(self):
        client=MagicMock()
        client.get_orders.return_value=[SimpleNamespace(id='b-buy'),SimpleNamespace(id='d-buy')]
        with patch('time.sleep'):
            self.assertEqual(b._cancel_open_buy_orders(client,'META','b-buy'),1)
        client.cancel_order_by_id.assert_called_once_with('b-buy')

    def test_entry_failure_does_not_skip_error_position(self):
        with patch.object(d,'ensure_schema'), patch('ultimate_v1.d_entry_filter.sample_tick',side_effect=RuntimeError('quotes down')), patch.object(d,'fetch_all',return_value=[{'symbol':'META'}]) as fetch, patch.object(d,'run_symbol',return_value='waiting_market_close_fill') as run:
            d.run_all()
        self.assertIn("'ERROR'",fetch.call_args.args[0])
        run.assert_called_once_with('META')

    def test_b_sell_remainder_uses_b_quantity_only(self):
        client=MagicMock()
        client.get_order_by_id.return_value=SimpleNamespace(filled_avg_price=100)
        row={'qty':3,'cost_price':90}
        conn=MagicMock()
        with patch.object(b,'_get_trading_client',return_value=client), patch.object(b,'_get_real_position_qty',return_value=23), patch.object(b,'_load_one_b_row',return_value=row), patch.object(b,'_submit_market_qty',return_value=SimpleNamespace(id='b-sell',status='new')), patch.object(b,'_reconcile_sell_fill',return_value=2), patch.object(b,'_update_ops_fields') as update:
            b._sell_qty_limit_ext(conn,'META',2,100,'test')
        self.assertEqual(update.call_args.kwargs['qty'],1)
