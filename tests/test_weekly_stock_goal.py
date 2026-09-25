import unittest
from unittest.mock import patch, MagicMock
from ultimate_v1 import web_app as web
from ultimate_v1 import return_goals as goals

class ReturnGoalTests(unittest.TestCase):
    def test_week_and_month_use_curve_and_distinct_targets(self):
        curve={'start_date':'2026-09-01','end_date':'2026-09-30','rows':[{'equity':1000},{'equity':1200}], 'return_fraction':.2}
        with patch('ultimate_v1.adjusted_returns.curve',return_value=curve), patch.object(goals,'settle_goals',return_value=(2,1)) as settle:
            for period,target in [('week',.05),('month',.20)]:
                result=web._period_return_goal(period)
                self.assertEqual(result['target'],target)
                self.assertEqual(result['current'],.20)
                self.assertEqual(result['failed_count'],1)
                self.assertNotIn('step',result)
                settle.assert_called_with(period,curve,target)

    def test_settles_success_failure_once_and_leaves_active_pending(self):
        import json
        cursor=MagicMock()
        records=[{'setting_key':str(i),'setting_value':json.dumps(dict(start='2020-01-01',end=end,target=.05,status='pending'))}
                 for i,end in enumerate(['2020-01-31','2020-02-29','2099-01-01'])]
        cursor.fetchall.side_effect=[records]
        with patch('ultimate_v1.adjusted_returns.curve',side_effect=lambda period,bounds,refresh: {'rows':[{}, {'created_at':__import__('datetime').datetime.combine(bounds[1], __import__('datetime').time())}], 'return_fraction': .06 if bounds[1].month == 1 else .01}), patch.object(goals,'db_conn') as db:
            db.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value=cursor
            self.assertEqual(goals.settle_goals('month',{'start_date':'2099-01-01','end_date':'2099-01-31'},.2),(1,1))
            updates=[c for c in cursor.execute.call_args_list if c.args[0].startswith('UPDATE')]
            self.assertEqual(len(updates),2)
            for c in updates:
                records[int(c.args[1][1])]['setting_value']=c.args[1][0]
            cursor.fetchall.side_effect=[records]
            cursor.execute.reset_mock()
            self.assertEqual(goals.settle_goals('month',{'start_date':'2099-01-01','end_date':'2099-01-31'},.2),(1,1))
            self.assertFalse(any(c.args[0].startswith('UPDATE') for c in cursor.execute.call_args_list))
