import unittest
from types import SimpleNamespace
from unittest.mock import patch
from ultimate_v1 import web_app as web

class AnnualGoalAccountTests(unittest.TestCase):
    def test_retirement_tracks_a_and_stock_reset_happens_once(self):
        settings={'ANNUAL_STOCK_START_EQUITY':'24000','ANNUAL_STOCK_COMPLETIONS':'2','ANNUAL_RETIREMENT_CURRENT':'1000'}
        allocation=SimpleNamespace(equity=3300,pool_brokers={'A':'retirement','B':'trading'},broker_snapshots={'retirement':{'equity':100},'trading':{'equity':3200}})
        def reset(equity):
            settings.update(ANNUAL_STOCK_START_EQUITY=str(equity),ANNUAL_STOCK_COMPLETIONS='0',ANNUAL_STOCK_BASIS='trading-v2')
        with patch.object(web,'_ensure_weekly_goal_reset'),patch.object(web,'get_app_setting',side_effect=lambda k,d='':settings.get(k,d)),patch.object(web,'_setting_float',side_effect=lambda k,d:float(settings.get(k,d))),patch.object(web,'_reset_stock_growth',side_effect=reset) as call:
            first={r['key']:r for r in web._annual_goals_payload(allocation)}
            web._annual_goals_payload(allocation)
        call.assert_called_once_with(3200)
        self.assertEqual(0,first['stock_growth']['current'])
        self.assertEqual(0,first['stock_growth']['completed_count'])
        self.assertEqual(100,first['retirement']['current'])
        self.assertNotIn('step',first['retirement'])

    def test_reset_fails_without_live_account_equity(self):
        with patch.object(web,'_ensure_weekly_goal_reset'),patch('ultimate_v1.alpaca_gateway.get_account_snapshot',return_value=None),patch.object(web,'_reset_stock_growth') as reset:
            self.assertFalse(web._advance_annual_goal('stock_growth')['ok'])
            reset.assert_not_called()

    def test_retirement_cannot_be_incremented_manually(self):
        with patch.object(web,'_ensure_weekly_goal_reset'):
            self.assertFalse(web._advance_annual_goal('retirement')['ok'])
