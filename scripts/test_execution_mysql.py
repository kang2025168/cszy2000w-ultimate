"""Integration checks against the disposable cszy_test database on port 13379 only."""
import os
from pathlib import Path
import socket
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.clear()
os.environ.update(CSZY_LOAD_DOTENV="0", DB_HOST="127.0.0.1", DB_PORT="13379",
                  DB_USER="root", DB_PASS="", DB_NAME="cszy_test", ALPACA_MODE="paper")
OriginalSocket = socket.socket

class TestSocket(OriginalSocket):
    def connect(self, address):
        if address != ("127.0.0.1", 13379):
            raise RuntimeError("Integration tests only allow disposable MySQL")
        return super().connect(address)
    def connect_ex(self, address):
        self.connect(address)
        return 0

socket.socket = TestSocket
from ultimate_v1.db import db_conn, fetch_one
from ultimate_v1.schema import ensure_schema
from ultimate_v1 import order_journal as journal, manual_ledger, d_grid


class ExecutionDatabaseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with db_conn() as conn:
            with conn.cursor() as cur:
                text = (ROOT / "mysql-init/001_create_tables.sql").read_text()
                for statement in text.split(";"):
                    if statement.strip().upper().startswith("CREATE TABLE"):
                        cur.execute(statement)
        ensure_schema()

    def setUp(self):
        with db_conn() as conn:
            with conn.cursor() as cur:
                for table in ("execution_orders", "stock_operations", "position_holdings", "d_grid_events", "d_grid_cycles", "d_grid_symbols"):
                    cur.execute(f"DELETE FROM {table}")

    def prepare(self, side="buy"):
        return journal.prepare("cszy-manual-test", "trading", "C", "MOCK", side,
                               {"preview": {"qty": 5}}, 500 if side == "buy" else 0)

    def order(self, qty, price=100, status="partially_filled"):
        return SimpleNamespace(id="broker-order", filled_qty=qty, filled_avg_price=price, status=status)

    def test_migration_is_versioned_and_repeatable(self):
        ensure_schema()
        self.assertEqual(1, fetch_one("SELECT COUNT(*) AS n FROM schema_migrations WHERE version=3")["n"])

    def test_repeated_and_partial_fills_book_exactly_once(self):
        self.prepare()
        for qty, price in [(2, 100), (2, 100), (5, 106), (5, 106)]:
            manual_ledger.apply_order("cszy-manual-test", self.order(qty, price, "filled" if qty == 5 else "partially_filled"))
        row = fetch_one("SELECT qty,cost_price FROM stock_operations WHERE stock_code='MOCK'")
        self.assertEqual(5, float(row["qty"]))
        self.assertEqual(106, float(row["cost_price"]))
        self.assertEqual(0, journal.reserved_for_pool("C"))

    def test_unfilled_order_never_creates_position(self):
        self.prepare()
        manual_ledger.apply_order("cszy-manual-test", self.order(0, 0, "new"))
        self.assertIsNone(fetch_one("SELECT qty FROM stock_operations WHERE stock_code='MOCK'"))
        self.assertEqual(500, journal.reserved_for_pool("C"))

    def test_database_error_rolls_back_booking_and_progress(self):
        self.prepare()
        with patch.object(manual_ledger, "upsert_buy_holding", side_effect=RuntimeError("simulated write failure")):
            with self.assertRaises(RuntimeError):
                manual_ledger.apply_order("cszy-manual-test", self.order(2))
        self.assertIsNone(fetch_one("SELECT qty FROM stock_operations WHERE stock_code='MOCK'"))
        self.assertEqual(0, float(journal.get_intent("cszy-manual-test")["accounted_qty"]))
        self.assertEqual(500, journal.reserved_for_pool("C"))

    def test_changed_idempotent_request_rejected(self):
        self.prepare()
        with self.assertRaises(ValueError):
            journal.prepare("cszy-manual-test", "trading", "C", "MOCK", "buy", {"preview": {"qty": 99}})

    def test_partial_cancel_books_then_releases_reservation(self):
        self.prepare()
        manual_ledger.apply_order("cszy-manual-test", self.order(2, 100, "canceled"))
        self.assertEqual(2, float(fetch_one("SELECT qty FROM stock_operations WHERE stock_code='MOCK'")["qty"]))
        self.assertEqual(0, journal.reserved_for_pool("C"))

    def test_same_symbol_strategy_lots_remain_isolated(self):
        self.prepare()
        journal.prepare("cszy-manual-other", "retirement", "A", "MOCK", "buy", {"preview": {"qty": 8}}, 800)
        manual_ledger.apply_order("cszy-manual-other", self.order(8, 120, "filled"))
        manual_ledger.apply_order("cszy-manual-test", self.order(2, 100, "filled"))
        self.assertEqual(8, float(fetch_one("SELECT qty FROM stock_operations WHERE stock_code='MOCK' AND stock_type='A'")["qty"]))
        self.assertEqual(2, float(fetch_one("SELECT qty FROM stock_operations WHERE stock_code='MOCK' AND stock_type='C'")["qty"]))

    def test_concurrent_manual_retries_submit_only_once(self):
        from concurrent.futures import ThreadPoolExecutor
        from unittest.mock import Mock
        from ultimate_v1 import manual_execution as web_app, trading_gate
        client = Mock()
        client.submit_order.return_value = self.order(5, 100, "filled")
        client.get_order_by_client_id.return_value = client.submit_order.return_value
        payload = dict(symbol="MOCK", pool="C", side="buy", size="1/4", order_type="limit", limit_price=100,
                       request_id="concurrent-test-request", execute=True)
        preview = dict(ok=True, symbol="MOCK", pool="C", side="buy", order_type="limit", qty=5, price=100, notional=500)
        with patch.object(web_app, "_plan_manual_stock_order", return_value=preview), patch.object(web_app.alpaca_gateway, "trading_client", return_value=client), patch.object(trading_gate, "can_open_position", return_value=(True, "ok")):
            with ThreadPoolExecutor(max_workers=2) as executor:
                results = list(executor.map(web_app._manual_stock_order_payload, [payload, payload]))
        self.assertTrue(all(r["ok"] for r in results))
        client.submit_order.assert_called_once()
        self.assertEqual(5, float(fetch_one("SELECT qty FROM stock_operations WHERE stock_code='MOCK'")["qty"]))

    def test_concurrent_requests_cannot_reuse_reserved_capital(self):
        from concurrent.futures import ThreadPoolExecutor
        from unittest.mock import Mock
        from ultimate_v1 import manual_execution as web_app, trading_gate
        client = Mock()
        client.submit_order.return_value = self.order(0, 0, "new")
        preview = dict(ok=True, symbol="MOCK", pool="C", side="buy", order_type="limit", qty=5, price=100, notional=500)
        payload = dict(symbol="MOCK", pool="C", side="buy", size="1/4", order_type="limit", limit_price=100, execute=True)
        requests = [{**payload, "request_id": f"separate-request-{n}"} for n in range(2)]
        def gate(pool, amount):
            return (amount <= 500 - journal.reserved_for_pool(pool), "available")
        with patch.object(web_app, "_plan_manual_stock_order", return_value=preview), patch.object(web_app.alpaca_gateway, "trading_client", return_value=client), patch.object(trading_gate, "can_open_position", side_effect=gate):
            with ThreadPoolExecutor(max_workers=2) as executor:
                results = list(executor.map(web_app._manual_stock_order_payload, requests))
        self.assertEqual(1, sum(bool(r["ok"]) for r in results))
        client.submit_order.assert_called_once()
        self.assertEqual(500, journal.reserved_for_pool("C"))

    def test_grid_error_is_durable_and_retains_recoverable_state(self):
        from datetime import datetime
        from zoneinfo import ZoneInfo
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO d_grid_symbols (symbol,enabled) VALUES ('MOCK',1)")
                cur.execute("INSERT INTO d_grid_cycles (symbol,state,cycle_no,buy_limit,buy_qty,pending_client_order_id) VALUES ('MOCK','BUY_SUBMITTING',1,100,5,'dgrid-test')")
        with patch.object(d_grid, "_runtime_bool", return_value=False), patch.object(d_grid, "trading_client"), patch.object(d_grid, "_submit_limit", side_effect=TimeoutError("simulated broker timeout")):
            with self.assertRaises(TimeoutError):
                d_grid.run_symbol("MOCK", now=datetime(2026,9,22,10,tzinfo=ZoneInfo("America/Los_Angeles")))
        row = fetch_one("SELECT state,last_error FROM d_grid_cycles WHERE symbol='MOCK'")
        self.assertEqual("BUY_SUBMITTING", row["state"])
        self.assertIn("simulated", row["last_error"])
        self.assertEqual(1, fetch_one("SELECT COUNT(*) AS n FROM d_grid_events")["n"])

    def test_grid_restart_queries_existing_submission_without_resubmit(self):
        from unittest.mock import Mock
        from datetime import datetime
        from zoneinfo import ZoneInfo
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO d_grid_symbols (symbol,enabled) VALUES ('MOCK',1)")
                cur.execute("INSERT INTO d_grid_cycles (symbol,state,cycle_no,buy_limit,buy_qty,pending_client_order_id) VALUES ('MOCK','BUY_SUBMITTING',1,100,5,'dgrid-test')")
        journal.prepare("dgrid-test", "trading", "D", "MOCK", "buy", {"symbol": "MOCK", "side": "buy", "qty": 5, "price": 100}, 500)
        journal.update("dgrid-test", state="submitting")
        client = Mock()
        client.get_order_by_client_id.return_value = SimpleNamespace(id="already-accepted", status="new")
        with patch.object(d_grid, "_runtime_bool", return_value=False), patch.object(d_grid, "trading_client", return_value=client):
            result = d_grid.run_symbol("MOCK", now=datetime(2026,9,22,10,tzinfo=ZoneInfo("America/Los_Angeles")))
        self.assertEqual("recovered_buy_working", result)
        client.submit_order.assert_not_called()
        self.assertEqual(500, journal.reserved_for_pool("D"))
        self.assertEqual("already-accepted", fetch_one("SELECT buy_order_id FROM d_grid_cycles WHERE symbol='MOCK'")["buy_order_id"])

    def test_sell_fill_replay_does_not_duplicate_realized_profit(self):
        self.prepare()
        manual_ledger.apply_order("cszy-manual-test", self.order(5, 100, "filled"))
        journal.prepare("cszy-manual-sell", "trading", "C", "MOCK", "sell", {"preview": {"qty": 2}})
        sell = SimpleNamespace(id="sell-order", filled_qty=2, filled_avg_price=110, status="filled")
        manual_ledger.apply_order("cszy-manual-sell", sell)
        manual_ledger.apply_order("cszy-manual-sell", sell)
        row = fetch_one("SELECT qty,realized_pnl FROM position_holdings WHERE symbol='MOCK' AND status='open'")
        self.assertEqual(3, float(row["qty"]))
        self.assertEqual(20, float(row["realized_pnl"]))

    def test_flatten_submission_does_not_clear_unfilled_position(self):
        from unittest.mock import Mock
        from ultimate_v1 import web_app, intraday_flatten, alpaca_gateway
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO stock_operations (stock_code,stock_type,strategy_group,qty,is_bought,cost_price) VALUES ('MOCK','D','D',5,1,100)")
        client = Mock()
        client.submit_order.return_value = self.order(0, 0, "new")
        client.get_order_by_client_id.return_value = client.submit_order.return_value
        with patch.object(web_app, "_stock_quote_payload", return_value={"last": 100}), patch.object(alpaca_gateway, "trading_client", return_value=client), patch.object(alpaca_gateway, "list_positions", return_value=[SimpleNamespace(symbol="MOCK", qty=5)]):
            self.assertEqual(1, intraday_flatten.flatten_d_positions(force=True))
            self.assertEqual(1, intraday_flatten.flatten_d_positions(force=True))
        client.submit_order.assert_called_once()
        row = fetch_one("SELECT qty,is_bought FROM stock_operations WHERE stock_code='MOCK' AND stock_type='D'")
        self.assertEqual(5, float(row["qty"]))
        self.assertEqual(1, row["is_bought"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
