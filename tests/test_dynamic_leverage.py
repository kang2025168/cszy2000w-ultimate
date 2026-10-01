import unittest
from datetime import datetime,timedelta,timezone
from types import SimpleNamespace as NS
from unittest.mock import patch,Mock
import json
from ultimate_v1 import dynamic_leverage as dl
from ultimate_v1.dynamic_reduction import reduction_qty

class DynamicLeverageTests(unittest.TestCase):
    def risk(self,**overrides):
        values=dict(market_trend='向上',vix=14,loss_days=0,max_drawdown=0,block_all_new=False,
                    daily_pnl_pct=0,qqq_change_pct=1,risk_preference='激进')
        return NS(**{**values,**overrides})

    def test_six_levels_and_uncovered_low_vix_down_day(self):
        for values,expected in [({},1.5),({'vix':16},1.4),({'vix':20},1),({'vix':24},.75),
             ({'market_trend':'横盘'},1.2),({'block_all_new':True},.5),
             ({'max_drawdown':.1},.5),({'loss_days':2},.75),({'loss_days':3},.5),
             ({'market_trend':'向下','vix':28},.5),({'qqq_change_pct':-1},1.4)]:
            self.assertEqual(dl.candidate(self.risk(**values))[0],expected)

    def test_preference_multipliers(self):
        self.assertEqual(dl.candidate(self.risk(vix=16,risk_preference='中性'))[0],1.26)
        self.assertEqual(dl.candidate(self.risk(vix=16,risk_preference='保守'))[0],1.05)

    def tick(self,state,minute=0,**kw):
        values=dict(target=1.5,reason='test',circuit=False,
            now=datetime(2026,9,30,15,tzinfo=timezone.utc)+timedelta(minutes=minute),market_open=True,valid=True)
        return dl.advance(state,**{**values,**kw})

    def test_immediate_drop_and_staged_recovery(self):
        state=self.tick({'ceiling':1.5},target=.5)
        self.assertEqual(state['ceiling'],.5)
        for base,expected in [(1,1.),(12,1.25),(23,1.5)]:
            for minute in (base,base+5,base+10): state=self.tick(state,minute)
            self.assertEqual(state['ceiling'],expected)

    def test_refreshes_and_restarts_do_not_accelerate_confirmation(self):
        state=self.tick({'ceiling':.5})
        for minute in (1,2,3,4): state=self.tick(json.loads(json.dumps(state)),minute)
        self.assertEqual(state['count'],1)
        self.assertEqual(state['ceiling'],.5)

    def test_intraday_circuit_latches_even_if_market_recovers(self):
        state=self.tick({'ceiling':1.5},target=.5,circuit=True)
        for minute in (10,20,30): state=self.tick(state,minute)
        self.assertEqual(state['ceiling'],.5)
        self.assertEqual(state['count'],0)

    def test_invalid_quotes_never_force_blind_liquidation(self):
        state=self.tick({'ceiling':1.5},valid=False,target=.5)
        self.assertEqual(state['ceiling'],1.5)
        self.assertFalse(state['valid'])

    def test_long_gap_or_market_close_resets_samples(self):
        state=self.tick({'ceiling':.5})
        state=self.tick(state,20)
        self.assertEqual(state['count'],1)
        state=self.tick(state,21,market_open=False)
        self.assertEqual(state['count'],0)

    def test_state_staleness_blocks_new_buys(self):
        now=datetime(2026,9,30,15,tzinfo=timezone.utc)
        raw=dict(ceiling=1.5,checked_at=now.timestamp()-181,valid=True,market_open=True)
        with patch.object(dl,'get_app_setting',return_value=json.dumps(raw)):
            self.assertFalse(dl.read_state(now)['allow_buy'])

    def test_reduction_never_sells_other_pool_or_more_than_broker_qty(self):
        self.assertEqual(reduction_qty(5,20,10000,100),5)
        self.assertEqual(reduction_qty(20,5,10000,100),5)
        self.assertEqual(reduction_qty(20,20,101,100),1.1)
        self.assertEqual(reduction_qty(20,20,101,0),0)

    def test_manual_setting_rejected_in_dynamic_mode_without_writes(self):
        from ultimate_v1 import web_app as web
        h=object.__new__(web.Handler);h.path='/api/risk_settings'
        h._read_json=Mock(return_value={'margin_usage':1.5});h._authenticated=Mock(return_value=True);h._send_json=Mock()
        with patch.dict('os.environ',{'DYNAMIC_RISK_ENABLED':'1'}),patch.object(web,'set_app_setting') as save:
            h.do_POST();save.assert_not_called()
            self.assertEqual(h._send_json.call_args.args[1],400)

    def test_trade_gate_fails_closed_before_other_checks(self):
        from ultimate_v1 import trading_gate
        with patch.dict('os.environ',{'DYNAMIC_RISK_ENABLED':'1'}),patch.object(dl,'read_state',return_value={'allow_buy':False}),patch.object(trading_gate,'can_open') as old:
            self.assertFalse(trading_gate.can_open_position('C',100,available_override=1000)[0]);old.assert_not_called()

    def test_refresh_recovers_corrupt_state_and_rejects_invalid_candidate(self):
        from contextlib import nullcontext
        from ultimate_v1 import alpaca_gateway, order_journal
        risk=self.risk(loss_days='bad')
        risk.market_reason='均线趋势'
        risk.vix_source='Yahoo实时/延迟'
        risk.account_metrics_source='Alpaca实时账户'
        quote=NS(last=100,bid=99,trade_timestamp=datetime.now(timezone.utc),quote_timestamp=None)
        with patch.object(alpaca_gateway,'trading_client') as client, \
             patch.object(alpaca_gateway,'get_latest_stock_quote',return_value=quote), \
             patch.object(order_journal,'execution_lock',return_value=nullcontext()), \
             patch.object(dl,'get_app_setting',return_value='{broken'), \
             patch.object(dl,'set_app_setting') as save:
            client.return_value.get_clock.return_value=NS(is_open=True)
            state=dl.refresh(risk)
            self.assertFalse(state['valid'])
            self.assertEqual(state['ceiling'],.5)
            save.assert_called_once()

    def test_valid_refresh_recovers_corrupt_state_without_immediate_full_leverage(self):
        from contextlib import nullcontext
        from ultimate_v1 import alpaca_gateway, order_journal
        risk=self.risk()
        risk.market_reason='均线趋势'
        risk.vix_source='Yahoo实时/延迟'
        risk.account_metrics_source='Alpaca实时账户'
        quote=NS(last=100,bid=99,trade_timestamp=datetime.now(timezone.utc),quote_timestamp=None)
        with patch.object(alpaca_gateway,'trading_client') as client, \
             patch.object(alpaca_gateway,'get_latest_stock_quote',return_value=quote), \
             patch.object(order_journal,'execution_lock',return_value=nullcontext()), \
             patch.object(dl,'get_app_setting',return_value='{broken'), \
             patch.object(dl,'set_app_setting'):
            client.return_value.get_clock.return_value=NS(is_open=True)
            state=dl.refresh(risk)
            self.assertTrue(state['valid'])
            self.assertEqual(state['ceiling'],1.)
            self.assertEqual(state['count'],1)
