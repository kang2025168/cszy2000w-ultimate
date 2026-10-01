"""Broker execution is mocked; these scenarios never place real orders."""
import unittest,json
from contextlib import ExitStack
from datetime import datetime,timezone
from types import SimpleNamespace as NS
from unittest.mock import Mock,patch
from ultimate_v1 import dynamic_reduction as dr

class DynamicReductionTests(unittest.TestCase):
    def setUp(self):
        self.stack=ExitStack();self.addCleanup(self.stack.close)
        def patcher(path,**kw): return self.stack.enter_context(patch(path,**kw))
        self.patcher=patcher
        patcher('ultimate_v1.dynamic_reduction.enabled',return_value=True)
        patcher('ultimate_v1.dynamic_reduction.read_state',return_value={'fresh':True,'valid':True,'allow_buy':True,'changed_at':datetime.now(timezone.utc).timestamp()})
        self.report=patcher('ultimate_v1.dynamic_reduction.report')
        self.allocation=NS(target_for=lambda g:100,weekly={})
        patcher('ultimate_v1.dynamic_reduction.pool_excess',return_value=(100,self.allocation))
        patcher('ultimate_v1.account_config.profile_for_pool',side_effect=lambda g:'retirement' if g=='A' else 'trading')
        self.client=Mock();self.client.get_clock.return_value=NS(is_open=True)
        self.client.get_orders.return_value=[]
        self.client.get_all_positions.return_value=[NS(symbol='SAME',qty=10)]
        patcher('ultimate_v1.alpaca_gateway.trading_client',return_value=self.client)
        now=datetime.now(timezone.utc)
        patcher('ultimate_v1.alpaca_gateway.get_latest_stock_quote',return_value=NS(bid=100,last=100,quote_timestamp=now,trade_timestamp=now))
        self.active=[];self.lots=[dict(stock_code='SAME',qty=4,last_order_id='own',ac_t_state='IDLE')]
        self.total=10
        def fetch(sql,args=()):
            if "LIKE 'cszy-dyn-" in sql: return self.active
            if 'SELECT order_id' in sql: return [{'order_id':'own'}]
            if 'SUM(qty)' in sql: return [dict(stock_code='SAME',qty=self.total)]
            return self.lots
        patcher('ultimate_v1.db.fetch_all',side_effect=fetch)
        self.prepare=patcher('ultimate_v1.order_journal.prepare',return_value={'state':'prepared'})
        self.submit=patcher('ultimate_v1.order_journal.submit_prepared',return_value=NS(id='new'))
        self.apply=patcher('ultimate_v1.manual_ledger.apply_order',return_value={'status':'partially_filled','filled_qty':.5,'filled_avg_price':100})
        self.update=patcher('ultimate_v1.order_journal.update')
        patcher('ultimate_v1.trade_history._record_manual_trade')

    def intent(self,market=False):
        return dict(client_order_id='cszy-dyn-B-test',state='submitted',symbol='SAME',request_json=json.dumps(dict(qty=1,price=100,market=market,started_at=0)))

    def test_durable_order_uses_own_pool_qty_not_account_total(self):
        self.assertEqual(dr.consume('B',locked=True),'dynamic_sell_submitted')
        self.assertEqual(self.prepare.call_args.args[2],'B')
        request=self.submit.call_args.args[1]
        self.assertEqual(float(request.qty),1)
        self.assertEqual(self.prepare.call_args.args[0],request.client_order_id)

    def test_partial_fill_timeout_cancels_without_replacement(self):
        self.active=[self.intent()];self.client.get_order_by_client_id.return_value=NS(id='old')
        self.assertEqual(dr.consume('B',locked=True),'dynamic_sell_pending')
        self.apply.assert_called_once()
        self.client.cancel_order_by_id.assert_called_once_with('old')
        self.submit.assert_not_called()

    def test_unknown_order_cannot_be_resubmitted(self):
        self.active=[self.intent()];self.active[0]['state']='unknown'
        self.client.get_order_by_client_id.side_effect=TimeoutError('broker unavailable')
        self.assertEqual(dr.consume('B',locked=True),'dynamic_reduction_error')
        self.submit.assert_not_called()

    def test_market_order_waits_for_fill_instead_of_replacing(self):
        self.active=[self.intent(market=True)];self.client.get_order_by_client_id.return_value=NS(id='old')
        dr.consume('B',locked=True)
        self.client.cancel_order_by_id.assert_not_called();self.submit.assert_not_called()

    def test_other_pool_order_is_not_canceled(self):
        self.client.get_orders.return_value=[NS(id='other',symbol='SAME',client_order_id='pool-C-test',side='sell')]
        dr.consume('B',locked=True)
        self.client.cancel_order_by_id.assert_not_called();self.submit.assert_not_called()

    def test_owned_order_cancel_confirm_before_new_sell(self):
        self.client.get_orders.return_value=[NS(id='own',symbol='SAME',client_order_id='pool-B-test',side='buy',status='new')]
        self.assertEqual(dr.consume('B',locked=True),'dynamic_cancel_own_orders')
        self.client.cancel_order_by_id.assert_called_once_with('own');self.submit.assert_not_called()

    def test_inventory_disagreement_blocks_sell(self):
        self.total=12
        dr.consume('B',locked=True);self.submit.assert_not_called()

    def test_active_c_t_cycle_must_reconcile_first(self):
        self.lots[0]['ac_t_state']='UP_T_HOLDING'
        dr.consume('C',locked=True);self.submit.assert_not_called()

    def test_market_closed_keeps_task_without_selling(self):
        self.client.get_clock.return_value=NS(is_open=False)
        self.assertEqual(dr.consume('B',locked=True),'dynamic_waiting_market')
        self.submit.assert_not_called()
