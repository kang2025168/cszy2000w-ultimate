import os
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from ultimate_v1 import account_config, order_journal, web_auth
from ultimate_v1.dashboard_cache import DashboardCache
from ultimate_v1.manual_ledger import fill_delta, lot_after_fill
from ultimate_v1.order_fills import wait_for_fill, status_text


class ExecutionSafetyTests(unittest.TestCase):
    def test_existing_position_never_counts_as_order_fill(self):
        client = Mock()
        client.get_order_by_id.return_value = SimpleNamespace(status="new", filled_qty=0, filled_avg_price=None)
        client.get_open_position.return_value = SimpleNamespace(qty=10, avg_entry_price=100)
        self.assertEqual((0, 0, "new"), wait_for_fill(client, "order", 0))
        client.get_open_position.assert_not_called()

    def test_order_error_does_not_use_position(self):
        client = Mock()
        client.get_order_by_id.side_effect = TimeoutError()
        self.assertEqual((0, 0, "unknown"), wait_for_fill(client, "order", 0))
        client.get_open_position.assert_not_called()

    def test_partial_fill_at_cancellation_is_preserved(self):
        client = Mock()
        client.get_order_by_id.return_value = SimpleNamespace(status="OrderStatus.CANCELED", filled_qty="2.5", filled_avg_price="101")
        self.assertEqual((2.5, 101, "canceled"), wait_for_fill(client, "order", 0))

    def test_incremental_fill_is_weighted_and_idempotent(self):
        self.assertEqual((3, 110), fill_delta(5, 106, 2, 200))
        self.assertEqual((0, 0), fill_delta(5, 106, 5, 530))
        self.assertEqual((0, 0), fill_delta(2, 100, 5, 530))
        self.assertEqual((0, 0), fill_delta(5, float("nan"), 0, 0))

    def test_long_sell_and_short_cover_accounting(self):
        self.assertEqual((6, 100, 40), lot_after_fill(10, 100, 4, 110, "sell"))
        self.assertEqual((-6, 100, 40), lot_after_fill(-10, 100, 4, 90, "buy"))
        self.assertEqual((0, 100, 100), lot_after_fill(-10, 100, 10, 90, "buy"))
        self.assertEqual((2, 90, 100), lot_after_fill(-10, 100, 12, 90, "buy"))

    def test_retirement_does_not_inherit_trading_credentials(self):
        with patch.dict(os.environ, {"PAPER_APCA_API_KEY_ID": "fake-key", "PAPER_APCA_API_SECRET_KEY": "fake-secret"}, clear=True):
            profile = account_config._profile_with_env(account_config.DEFAULT_CONFIG, "retirement")
            self.assertEqual("", profile["key_id"])
            self.assertEqual("", profile["secret_key"])

    def test_partial_credentials_are_rejected(self):
        with patch.dict(os.environ, {"RETIREMENT_APCA_API_KEY_ID": "fake", "PAPER_APCA_API_SECRET_KEY": "other"}, clear=True):
            with self.assertRaises(RuntimeError):
                account_config._profile_with_env(account_config.DEFAULT_CONFIG, "retirement")

    def test_duplicate_profiles_are_rejected(self):
        env = {"PAPER_APCA_API_KEY_ID": "same", "PAPER_APCA_API_SECRET_KEY": "secret",
               "RETIREMENT_APCA_API_KEY_ID": "same", "RETIREMENT_APCA_API_SECRET_KEY": "secret"}
        with patch.dict(os.environ, env, clear=True), patch.object(account_config, "load_account_config", return_value=account_config.DEFAULT_CONFIG):
            with self.assertRaises(RuntimeError):
                account_config.credentials_for_profile("retirement")

    def test_config_database_error_is_not_silent_fallback(self):
        with patch.object(account_config, "get_app_setting", side_effect=RuntimeError("offline")):
            with self.assertRaises(RuntimeError):
                account_config.load_account_config()

    def test_missing_password_denies_session(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertFalse(web_auth.verify_token("anything"))
            self.assertEqual("", web_auth.issue_token())

    def test_sessions_expire_and_detect_tampering(self):
        with patch.dict(os.environ, {"DASHBOARD_LOGIN_PASSWORD": "test-password", "DASHBOARD_SESSION_SECONDS": "60"}, clear=True):
            with patch.object(web_auth.time, "time", return_value=1000):
                token = web_auth.issue_token()
                self.assertTrue(web_auth.verify_token(token))
                self.assertFalse(web_auth.verify_token(token + "x"))
                self.assertNotEqual(token, web_auth.issue_token())
            with patch.object(web_auth.time, "time", return_value=1061):
                self.assertFalse(web_auth.verify_token(token))

    def test_login_is_rate_limited(self):
        with patch.object(web_auth.time, "monotonic", return_value=100):
            self.assertTrue(all(web_auth.allow_login("test-address") for _ in range(10)))
            self.assertFalse(web_auth.allow_login("test-address"))

    def test_ambiguous_submit_recovers_without_second_submit(self):
        client = Mock()
        client.submit_order.side_effect = TimeoutError()
        client.get_order_by_client_id.return_value = SimpleNamespace(id="broker-id", status="new")
        with patch.object(order_journal, "update"):
            order = order_journal.submit_prepared(client, object(), {"client_order_id": "test-id", "state": "prepared"})
        self.assertEqual("broker-id", order.id)
        client.submit_order.assert_called_once()

    def test_restart_in_submitting_state_only_queries(self):
        client = Mock()
        client.get_order_by_client_id.side_effect = TimeoutError()
        with self.assertRaises(TimeoutError):
            order_journal.submit_prepared(client, object(), {"client_order_id": "test-id", "state": "submitting"})
        client.submit_order.assert_not_called()

    def test_snapshot_reads_do_not_repeat_broker_work(self):
        loader = Mock(return_value={"ok": True, "balance": 10})
        cache = DashboardCache({"capital": loader})
        self.assertFalse(cache.get("capital")["ok"])
        cache.refresh()
        for _ in range(10):
            self.assertEqual(10, cache.get("capital")["balance"])
        loader.assert_called_once()
        with patch("ultimate_v1.dashboard_cache.time.monotonic", return_value=10**15):
            self.assertFalse(cache.get("capital")["ok"])

    def test_handler_rejects_unauthenticated_request(self):
        from ultimate_v1.web_app import Handler
        handler = Handler.__new__(Handler)
        handler.headers = {}
        with patch.dict(os.environ, {}, clear=True):
            self.assertFalse(handler._authenticated())

    def test_manual_preview_ignores_browser_market_price(self):
        from ultimate_v1 import web_app
        capital = {"ok": True, "available": {"C": 5000}, "pool_brokers": {"C": "trading"},
                   "broker_snapshots": {"trading": {"buying_power": 5000}}}
        with patch.object(web_app, "_stock_quote_payload", return_value={"last": 100}), patch.object(web_app, "_allocation_payload", return_value=capital):
            result = web_app._plan_manual_stock_order({"symbol": "MOCK", "side": "buy", "pool": "C", "order_type": "market", "client_last": 1})
        self.assertEqual(100, result["price"])
        self.assertEqual(12.5, result["qty"])

    def test_manual_sell_cannot_take_other_strategy_lot(self):
        from ultimate_v1 import web_app
        with patch.object(web_app, "_stock_quote_payload", return_value={"last": 100}), patch.object(web_app.alpaca_gateway, "list_positions", return_value=[SimpleNamespace(symbol="MOCK", qty=10)]), patch("ultimate_v1.manual_execution.fetch_all", side_effect=[[{"qty": 4}], []]), patch("ultimate_v1.manual_execution.profile_for_pool", return_value="trading"):
            result = web_app._plan_manual_stock_order({"symbol": "MOCK", "side": "sell", "pool": "C", "size": "full"})
        self.assertEqual(4, result["qty"])

    def test_cross_origin_json_rejected(self):
        from ultimate_v1.web_app import Handler
        handler = Handler.__new__(Handler)
        handler.headers = {"Content-Length": "2", "Content-Type": "application/json", "Host": "localhost", "Origin": "https://example.invalid"}
        with self.assertRaises(ValueError):
            handler._read_json()

    def test_buying_power_failure_does_not_reuse_old_balance(self):
        from app.bots import runtime_core
        with patch.object(runtime_core, "_get_alpaca_client", side_effect=TimeoutError()), patch.object(runtime_core, "_cached_buying_power", 100000):
            self.assertEqual(0, runtime_core.get_buying_power())


if __name__ == "__main__":
    unittest.main()
