import unittest
from unittest.mock import patch
from ultimate_v1 import state_store as s

class SettingCacheTests(unittest.TestCase):
    def setUp(self):
        s._APP_SETTING_CACHE.clear()

    def test_missing_default_is_per_call(self):
        with patch.object(s,'fetch_one',return_value=None) as read:
            self.assertEqual('one',s.get_app_setting('ANNUAL_RETIREMENT_TARGET','one'))
            self.assertEqual('two',s.get_app_setting('ANNUAL_RETIREMENT_TARGET','two'))
            self.assertEqual(1,read.call_count)

    def test_risk_changes_are_not_cached(self):
        with patch.object(s,'fetch_one',side_effect=[{'setting_value':'1'},{'setting_value':'0'}]) as read:
            self.assertEqual('1',s.get_app_setting('RISK_B_POOL_ENABLED'))
            self.assertEqual('0',s.get_app_setting('RISK_B_POOL_ENABLED'))
            self.assertEqual(2,read.call_count)

    def test_unknown_state_keys_are_fresh(self):
        with patch.object(s,'fetch_one',return_value=None) as read:
            s.get_app_setting('MONTHLY_INVEST_LAST_RUN_MONTH')
            s.get_app_setting('MONTHLY_INVEST_LAST_RUN_MONTH')
            self.assertEqual(2,read.call_count)
