import unittest
from types import SimpleNamespace
from unittest.mock import patch
from ultimate_v1 import web_app as web

class AnnualGoalAccountTests(unittest.TestCase):
    def test_retirement_tracks_a_and_month_replaces_growth(self):
        allocation=SimpleNamespace(pool_brokers={'A':'retirement'},broker_snapshots={'retirement':{'equity':100}})
        with patch.object(web,'_ensure_weekly_goal_reset'), patch.object(web,'_setting_float',side_effect=lambda k,d:d), patch.object(web,'_weekly_stock_goal',return_value={'key':'weekly_stock'}), patch.object(web,'_period_return_goal',return_value={'key':'stock_growth','target':.2}):
            rows={r['key']:r for r in web._annual_goals_payload(allocation)}
            self.assertEqual(rows['retirement']['current'],100)
            self.assertEqual(rows['stock_growth']['target'],.2)

    def test_reset_fails_without_live_account_equity(self):
        with patch.object(web,'_ensure_weekly_goal_reset'),patch('ultimate_v1.alpaca_gateway.get_account_snapshot',return_value=None),patch.object(web,'_reset_stock_growth') as reset:
            self.assertFalse(web._advance_annual_goal('stock_growth')['ok'])
            reset.assert_not_called()

    def test_retirement_cannot_be_incremented_manually(self):
        with patch.object(web,'_ensure_weekly_goal_reset'):
            self.assertFalse(web._advance_annual_goal('retirement')['ok'])
