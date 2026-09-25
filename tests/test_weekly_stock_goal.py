import unittest
from datetime import date
from types import SimpleNamespace
from unittest.mock import patch
from ultimate_v1 import web_app as web


class WeeklyStockGoalTests(unittest.TestCase):
    def test_baseline_persists_and_new_week_rolls_over(self):
        settings = {}
        allocation = SimpleNamespace(pool_brokers={'B': 'trading'}, broker_snapshots={
            'trading': {'equity': 1050}, 'retirement': {'equity': 90000}})
        with patch.object(web, 'equity_curve_bounds', return_value=(date(2026,9,18),date(2026,9,25))) as now, \
             patch.object(web, '_weekly_stock_completions', return_value=(1,True)), \
             patch.object(web, 'get_app_setting', side_effect=lambda k,d: settings.get(k,d)), \
             patch.object(web, 'set_app_setting', side_effect=lambda k,v: settings.update({k:v})), \
             patch.object(web, 'fetch_all', return_value=[{'equity':1000}]) as fetch:
            goal = web._weekly_stock_goal(allocation)
            self.assertAlmostEqual(goal['current'], .05)
            self.assertIn('09/18–09/25', goal['desc'])
            self.assertEqual(goal['completed_count'], 1)
            self.assertEqual(goal['status_label'], '本周已达成')
            web._weekly_stock_goal(allocation)
            self.assertEqual(fetch.call_count, 1)
            self.assertEqual(fetch.call_args.args[1][0], 'trading')
            now.return_value = (date(2026,9,25),date(2026,10,2))
            web._weekly_stock_goal(allocation)
            self.assertEqual(fetch.call_count, 2)
            self.assertEqual(len(settings), 2)

    def test_missing_history_and_missing_account_are_explicit(self):
        allocation = SimpleNamespace(pool_brokers={}, broker_snapshots={'trading': {'equity':1000}})
        with patch.object(web, '_weekly_stock_completions', return_value=(0,False)), \
             patch.object(web, 'get_app_setting', return_value=''), \
             patch.object(web, 'set_app_setting') as save, \
             patch.object(web, 'fetch_all', return_value=[]):
            goal = web._weekly_stock_goal(allocation)
            self.assertEqual(goal['current'], 0)
            self.assertIn('首次记录', goal['status_label'])
            allocation.broker_snapshots = {}
            goal = web._weekly_stock_goal(allocation)
            self.assertIsNone(goal['current'])
            self.assertEqual(save.call_count, 1)

    def test_completion_is_saved_once_per_week_and_survives_pullback(self):
        from unittest.mock import MagicMock
        keys = set()
        cursor = MagicMock()
        def execute(sql, args):
            if sql.startswith('INSERT IGNORE'):
                keys.add(args[0])
        cursor.execute.side_effect = execute
        cursor.fetchall.side_effect = lambda: [{'setting_key': k} for k in keys]
        connection = MagicMock()
        connection.cursor.return_value.__enter__.return_value = cursor
        with patch.object(web, 'db_conn') as db:
            db.return_value.__enter__.return_value = connection
            self.assertEqual(web._weekly_stock_completions('trading', '2026-09-21', True), (1, True))
            self.assertEqual(web._weekly_stock_completions('trading', '2026-09-21', True), (1, True))
            self.assertEqual(web._weekly_stock_completions('trading', '2026-09-21', False), (1, True))
            self.assertEqual(web._weekly_stock_completions('trading', '2026-09-28', False), (1, False))
            self.assertEqual(web._weekly_stock_completions('trading', '2026-09-28', True), (2, True))
