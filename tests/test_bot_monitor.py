import unittest
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from ultimate_v1.bot_monitor import build_monitor


class BotMonitorTests(unittest.TestCase):
    def row(self, *, age=10, enabled=1, status='running', message='', processes=(), interval=60, risk=None):
        now = datetime(2026, 9, 30, 12, tzinfo=ZoneInfo('America/Los_Angeles'))
        result = build_monitor(
            [{'bot_name':'b_buy_bot', 'status':status, 'last_message':message,
              'last_seen_at': None if age is None else (now-timedelta(seconds=age)).replace(tzinfo=None)}],
            [{'bot_name':'b_buy_bot', 'enabled':enabled}], list(processes),
            timezone='America/Los_Angeles', intervals={'b_buy_bot':interval}, risk=risk, now=now)
        return next(r for r in result['rows'] if r['bot_name']=='b_buy_bot')

    def test_external_process_uses_heartbeat(self):
        self.assertEqual(self.row()['status'], 'ok')

    def test_observed_dead_process_overrides_fresh_heartbeat(self):
        self.assertEqual(self.row(processes=[{'bot_name':'b_buy_bot','running':False}])['status'], 'error')

    def test_disabled_stale_is_not_failure(self):
        self.assertEqual(self.row(age=9999, enabled=0)['status'], 'off')

    def test_market_closed_only_idle_with_fresh_heartbeat(self):
        self.assertEqual(self.row(message='market_closed')['status'], 'idle')
        self.assertEqual(self.row(message='market_closed', age=9999)['status'], 'error')

    def test_long_interval_not_false_alarm(self):
        self.assertEqual(self.row(age=1000, interval=900)['status'], 'ok')

    def test_error_overrides_idle_message(self):
        self.assertEqual(self.row(status='failed', message='market_closed')['status'], 'error')

    def test_missing_or_future_time_is_unknown(self):
        for age in (None, -60):
            self.assertEqual(self.row(age=age)['status'], 'unknown')

    def test_risk_block_is_separate_from_health(self):
        row = self.row(risk={'block_b_buy':1})
        self.assertEqual(row['status'], 'ok')
        self.assertIn('风控限制', row['restriction'])
        result = build_monitor([],[],[],timezone='UTC',risk={'block_all_new':1})
        sell = next(r for r in result['rows'] if r['bot_name']=='b_sell_bot')
        self.assertEqual(sell['restriction'], '')

    def test_warning_and_unknown_not_green(self):
        self.assertEqual(self.row(status='warning')['status'], 'warn')
        self.assertEqual(self.row(status='starting')['status'], 'unknown')
