"""Isolated risk-panel audit; no broker or database calls are permitted."""
import unittest
from unittest.mock import Mock, patch
from types import SimpleNamespace as NS
from ultimate_v1 import capital_manager as cm, exposure_manager as em, web_app as web


def risk(**overrides):
    values=dict(market_trend='向上',vix=14,qqq_change_pct=1,loss_days=0,
                max_drawdown=0,risk_preference='激进',block_all_new=False,
                recommended_exposure=1)
    values.update(overrides)
    return NS(**values)


class RiskPanelAudit(unittest.TestCase):
    def request(self, payload, authenticated=True, password=True):
        handler=object.__new__(web.Handler)
        handler.path='/api/risk_settings'
        handler._read_json=Mock(return_value=payload)
        handler._authenticated=Mock(return_value=authenticated)
        handler._check_password=Mock(return_value=password)
        handler._send_json=Mock()
        return handler

    def test_all_manual_levels_scale_bcd_but_not_a(self):
        for level in (1,1.1,1.2,1.3,1.4,1.5):
            with self.subTest(level=level), patch.object(cm,'get_app_setting',side_effect=lambda k,d: {'RISK_MARGIN_MODE':'MANUAL','RISK_TOTAL_CAPITAL_PCT':str(level)}.get(k,d)),patch.object(cm,'get_risk_state',return_value=risk()),patch.object(cm,'pool_enabled_settings',return_value=dict.fromkeys('ABCD',True)):
                total,pools=cm._risk_percents()
                self.assertAlmostEqual(total,level)
                self.assertEqual(cm._risk_target_for_group('A',100,total,pools),100)
                for group in 'BCD':
                    self.assertAlmostEqual(cm._risk_target_for_group(group,100,total,pools),100*level)

    def test_manual_still_respects_market_target(self):
        with patch.object(cm,'get_app_setting',side_effect=lambda k,d: {'RISK_MARGIN_MODE':'MANUAL','RISK_TOTAL_CAPITAL_PCT':'1.5'}.get(k,d)),patch.object(cm,'get_risk_state',return_value=risk(recommended_exposure=.35)),patch.object(cm,'pool_enabled_settings',return_value=dict.fromkeys('ABCD',True)):
            self.assertAlmostEqual(cm._risk_percents()[0],.525)

    def test_auto_preferences_and_defensive_boundaries(self):
        for preference,expected in [('保守',1.1),('中性',1.3),('激进',1.5)]:
            self.assertEqual(cm._auto_margin_usage_pct(risk(risk_preference=preference))[0],expected)
        for change in [dict(vix=28),dict(loss_days=2),dict(max_drawdown=.1),dict(market_trend='向下')]:
            self.assertEqual(cm._auto_margin_usage_pct(risk(**change))[0],1)

    def test_each_manual_level_saved_and_only_suggestion_generated(self):
        for level in (1,1.1,1.2,1.3,1.4,1.5):
            h=self.request({'margin_usage':level,'margin_mode':'MANUAL'})
            with patch.object(web,'set_app_setting') as save,patch.object(web,'get_risk_state'),patch.object(web,'write_risk_state'),patch.object(web,'refresh_exposure_plan') as refresh:
                h.do_POST()
                save.assert_any_call('RISK_TOTAL_CAPITAL_PCT',f'{level:.1f}')
                refresh.assert_called_once_with(mode='SUGGEST',execute=True)
                self.assertTrue(h._send_json.call_args.args[0]['ok'])

    def test_preference_and_auto_selection(self):
        for payload in [{'risk_preference':x} for x in ['保守','中性','激进']]+[{'margin_mode':'AUTO'}]:
            h=self.request(payload)
            with patch.object(web,'set_app_setting'),patch.object(web,'get_risk_state'),patch.object(web,'write_risk_state'),patch.object(web,'refresh_exposure_plan'):
                h.do_POST()
                self.assertTrue(h._send_json.call_args.args[0]['ok'])

    def test_unauthenticated_request_never_writes(self):
        h=self.request({'margin_usage':1.5},authenticated=False)
        with patch.object(web,'set_app_setting') as save:
            h.do_POST(); save.assert_not_called()
            self.assertEqual(h._send_json.call_args.args[1],401)

    def test_invalid_leverage_alone_rejected(self):
        for value in [0,2,-1,'nan','inf','bad']:
            h=self.request({'margin_usage':value})
            with patch.object(web,'set_app_setting') as save:
                h.do_POST();save.assert_not_called()
                self.assertEqual(h._send_json.call_args.args[1],400)

    def test_suggest_mode_never_submits_orders(self):
        with patch.object(em,'_submit_stock_order') as submit:
            self.assertEqual(em.execute_exposure_plan(NS(mode='SUGGEST',actions=[{'side':'buy'}]))[0]['status'],'planned')
            submit.assert_not_called()

    def test_global_block_caps_auto_leverage(self):
        # Confirmed defect: code reads block_all instead of RiskState.block_all_new.
        self.assertEqual(cm._auto_margin_usage_pct(risk(block_all_new=True))[0],1)

    def test_invalid_combined_request_is_not_partially_saved(self):
        # Confirmed defect: mode is saved before the leverage value is validated.
        h=self.request({'margin_mode':'MANUAL','margin_usage':2})
        with patch.object(web,'set_app_setting') as save:
            h.do_POST(); self.assertEqual(h._send_json.call_args.args[1],400)
            save.assert_not_called()

    def test_clear_requires_password_and_preview_never_submits(self):
        for valid in (False,True):
            h=self.request({'dry_run':True},password=valid);h.path='/api/clear_position'
            with patch.object(web.alpaca_gateway,'submit_current_price_limit_sell_all',return_value={}) as clear:
                h.do_POST()
                if valid: clear.assert_called_once_with(dry_run=True)
                else: clear.assert_not_called()

    def test_clear_preview_broker_orders_not_called(self):
        gateway=web.alpaca_gateway
        client=Mock();client.get_all_positions.return_value=[NS(symbol='QQQ',qty='0.2',current_price=700,asset_class='us_equity')]
        with patch.object(gateway,'trading_client',return_value=client),patch.object(gateway,'get_latest_stock_price',return_value=700):
            result=gateway.submit_current_price_limit_sell_all(dry_run=True)
            client.submit_order.assert_not_called()
            self.assertEqual(result['count'],1)

    def test_market_metrics_are_forwarded_without_recalculation(self):
        state=NS(enabled=True,mode='RISK_OFF',daily_pnl_pct=-.03,loss_days=1,max_drawdown=.04,
                 risk_multiplier=.5,block_all_new=False,block_a=False,block_b=True,block_c=False,block_d=True,
                 suggest_mode=None,reason='daily loss',market_trend='向上',market_reason='test',
                 qqq_price=739.51,qqq_change_pct=.21,vix=16.3,risk_preference='激进',
                 allocation_mode='动态分仓',recommended_exposure=1,recommended_weights={},
                 account_metrics_source='test',vix_source='test')
        with patch.object(web,'get_risk_state',return_value=state):
            payload=web._risk_payload()
        for field in ('qqq_price','qqq_change_pct','vix','market_trend','block_b','block_d'):
            self.assertEqual(payload[field],getattr(state,field))

    def test_clear_live_path_uses_limit_order_and_preserves_fractional_qty(self):
        gateway=web.alpaca_gateway
        client=Mock();client.get_all_positions.return_value=[NS(symbol='QQQ',qty='0.2',current_price=700,asset_class='us_equity')]
        client.submit_order.return_value=NS(id='fake',status='accepted')
        with patch.object(gateway,'trading_client',return_value=client),patch.object(gateway,'get_latest_stock_price',return_value=700):
            result=gateway.submit_current_price_limit_sell_all(dry_run=False)
        order=client.submit_order.call_args.kwargs['order_data']
        self.assertEqual(float(order.qty),.2)
        self.assertEqual(float(order.limit_price),700)
        self.assertEqual(result['ok_count'],1)
