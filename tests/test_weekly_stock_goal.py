import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch
from ultimate_v1 import web_app as web


class WeeklyStockGoalTests(unittest.TestCase):
    def test_baseline_persists_and_new_week_rolls_over(self):
        settings = {}
        allocation = SimpleNamespace(pool_brokers={'B': 'trading'}, broker_snapshots={
            'trading': {'equity': 1050}, 'retirement': {'equity': 90000}})
        with patch.object(web, '_now_market_tz', return_value=datetime(2026, 9, 24)) as now, \
             patch.object(web, 'get_app_setting', side_effect=lambda k,d: settings.get(k,d)), \
             patch.object(web, 'set_app_setting', side_effect=lambda k,v: settings.update({k:v})), \
             patch.object(web, 'fetch_all', return_value=[{'equity':1000}]) as fetch:
            goal = web._weekly_stock_goal(allocation)
            self.assertAlmostEqual(goal['current'], .05)
            self.assertEqual(goal['status_label'], '已达成')
            web._weekly_stock_goal(allocation)
            self.assertEqual(fetch.call_count, 1)
            self.assertEqual(fetch.call_args.args[1][0], 'trading')
            now.return_value = datetime(2026, 9, 28)
            web._weekly_stock_goal(allocation)
            self.assertEqual(fetch.call_count, 2)
            self.assertEqual(len(settings), 2)

    def test_missing_history_and_missing_account_are_explicit(self):
        allocation = SimpleNamespace(pool_brokers={}, broker_snapshots={'trading': {'equity':1000}})
        with patch.object(web, 'get_app_setting', return_value=''), \
             patch.object(web, 'set_app_setting') as save, \
             patch.object(web, 'fetch_all', return_value=[]):
            goal = web._weekly_stock_goal(allocation)
            self.assertEqual(goal['current'], 0)
            self.assertIn('首次记录', goal['status_label'])
            allocation.broker_snapshots = {}
            goal = web._weekly_stock_goal(allocation)
            self.assertIsNone(goal['current'])
            self.assertEqual(save.call_count, 1)
