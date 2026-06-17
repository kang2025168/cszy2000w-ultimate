from __future__ import annotations

import unittest
from datetime import timedelta


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.rowcount = 1

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, args=()):
        self.conn.executed.append((sql, args))

    def executemany(self, sql, args=()):
        self.conn.executed.append((sql, args))

    def fetchone(self):
        return self.conn.fetchone_result

    def fetchall(self):
        return self.conn.fetchall_result


class FakeConn:
    def __init__(self, fetchone_result=None, fetchall_result=None):
        self.executed = []
        self.fetchone_result = fetchone_result or {"n": 1}
        self.fetchall_result = fetchall_result or []
        self.commits = 0

    def cursor(self, *_args, **_kwargs):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1


class FakeAccount:
    buying_power = "999999"


class FakePosition:
    qty = "10"


class FakeClient:
    def get_account(self):
        return FakeAccount()

    def get_open_position(self, _symbol):
        return FakePosition()


class BCStrategyFlowTests(unittest.TestCase):
    def test_b_buy_and_sell_round_reach_execution_functions(self):
        import app.bots.runtime_core as tb
        import app.bots.split_core as sc

        calls = []
        originals = {
            "ensure_conn_alive": tb.ensure_conn_alive,
            "load_rows": tb.load_rows,
            "refresh_buy_gate": tb.refresh_buy_gate,
            "get_market_gate": tb.get_market_gate,
            "safe_call": tb.safe_call,
            "strategy_B_rank_and_confirm": tb.strategy_B_rank_and_confirm,
            "_buy_one": sc._buy_one,
            "_sell_one": sc._sell_one,
            "sleep": sc.t.sleep,
            "uniform": sc.random.uniform,
        }
        try:
            tb.ensure_conn_alive = lambda conn: conn

            def fake_rows(_conn, mode):
                if mode == "buy":
                    return [
                        {
                            "stock_code": "MOCKB",
                            "stock_type": "B",
                            "is_bought": 0,
                            "can_buy": 1,
                            "can_sell": 0,
                        }
                    ]
                return [
                    {
                        "stock_code": "MOCKB",
                        "stock_type": "B",
                        "is_bought": 1,
                        "can_buy": 0,
                        "can_sell": 1,
                    }
                ]

            tb.load_rows = fake_rows
            tb.refresh_buy_gate = lambda force=False: True
            tb.get_market_gate = lambda _conn: 1
            tb.safe_call = lambda fn, *args, **kwargs: fn(*args, **kwargs)
            tb.strategy_B_rank_and_confirm = lambda codes: list(codes)
            sc._buy_one = lambda code, stype: calls.append(("buy", code, stype)) or True
            sc._sell_one = lambda code, stype, phase: calls.append(("sell", code, stype, phase)) or True
            sc.t.sleep = lambda _seconds: None
            sc.random.uniform = lambda *_args: 0

            control = {"global_buy_enabled": 1, "strategy_b_enabled": 1, "sell_only_mode": 0}
            config = sc.SplitBotConfig("buy", ("B",), 0, 0)
            conn = FakeConn(fetchone_result={"n": 1})

            _, buy_traded = sc.run_buy_round(conn, config, "regular", control)
            _, sell_traded = sc.run_sell_round(conn, sc.SplitBotConfig("sell", ("B",), 0, 0), "regular")

            self.assertTrue(buy_traded)
            self.assertTrue(sell_traded)
            self.assertIn(("buy", "MOCKB", "B"), calls)
            self.assertIn(("sell", "MOCKB", "B", "regular"), calls)
        finally:
            tb.ensure_conn_alive = originals["ensure_conn_alive"]
            tb.load_rows = originals["load_rows"]
            tb.refresh_buy_gate = originals["refresh_buy_gate"]
            tb.get_market_gate = originals["get_market_gate"]
            tb.safe_call = originals["safe_call"]
            tb.strategy_B_rank_and_confirm = originals["strategy_B_rank_and_confirm"]
            sc._buy_one = originals["_buy_one"]
            sc._sell_one = originals["_sell_one"]
            sc.t.sleep = originals["sleep"]
            sc.random.uniform = originals["uniform"]

    def test_b_buy_round_blocks_when_risk_gate_is_closed(self):
        import app.bots.runtime_core as tb
        import app.bots.split_core as sc

        calls = []
        originals = {
            "ensure_conn_alive": tb.ensure_conn_alive,
            "load_rows": tb.load_rows,
            "refresh_buy_gate": tb.refresh_buy_gate,
            "get_market_gate": tb.get_market_gate,
            "safe_call": tb.safe_call,
            "strategy_B_rank_and_confirm": tb.strategy_B_rank_and_confirm,
            "_buy_one": sc._buy_one,
            "sleep": sc.t.sleep,
            "uniform": sc.random.uniform,
        }
        try:
            tb.ensure_conn_alive = lambda conn: conn
            tb.load_rows = lambda _conn, _mode: [{"stock_code": "MOCKB", "stock_type": "B", "can_buy": 1}]
            tb.refresh_buy_gate = lambda force=False: True
            tb.get_market_gate = lambda _conn: 1
            tb.safe_call = lambda fn, *args, **kwargs: fn(*args, **kwargs)
            tb.strategy_B_rank_and_confirm = lambda codes: list(codes)
            sc._buy_one = lambda code, stype: calls.append(("buy", code, stype)) or True
            sc.t.sleep = lambda _seconds: None
            sc.random.uniform = lambda *_args: 0

            config = sc.SplitBotConfig("buy", ("B",), 0, 0)
            blocked_cases = [
                ("regular", {"global_buy_enabled": 0, "strategy_b_enabled": 1, "sell_only_mode": 0}),
                ("regular", {"global_buy_enabled": 1, "strategy_b_enabled": 0, "sell_only_mode": 0}),
                ("regular", {"global_buy_enabled": 1, "strategy_b_enabled": 1, "sell_only_mode": 1}),
                ("closed", {"global_buy_enabled": 1, "strategy_b_enabled": 1, "sell_only_mode": 0}),
            ]

            for phase, control in blocked_cases:
                _, traded = sc.run_buy_round(FakeConn(), config, phase, control)
                self.assertFalse(traded)
            self.assertEqual([], calls)
        finally:
            tb.ensure_conn_alive = originals["ensure_conn_alive"]
            tb.load_rows = originals["load_rows"]
            tb.refresh_buy_gate = originals["refresh_buy_gate"]
            tb.get_market_gate = originals["get_market_gate"]
            tb.safe_call = originals["safe_call"]
            tb.strategy_B_rank_and_confirm = originals["strategy_B_rank_and_confirm"]
            sc._buy_one = originals["_buy_one"]
            sc.t.sleep = originals["sleep"]
            sc.random.uniform = originals["uniform"]

    def test_c_idle_state_buys_extra_lot_when_up_trigger_hits(self):
        import app.strategy_ac_t as ac

        updates = []
        originals = {
            "DRY_RUN": ac.DRY_RUN,
            "get_latest_stock_price": ac.get_latest_stock_price,
            "_submit_limit_and_wait": ac._submit_limit_and_wait,
        }
        try:
            ac.DRY_RUN = True
            ac.get_latest_stock_price = lambda _symbol: 101.5
            ac._submit_limit_and_wait = (
                lambda _client, symbol, qty, side, price: ac.FillResult(
                    True, f"mock-{side.lower()}-{symbol}", "filled", qty, price
                )
            )

            conn = FakeConn()
            original_execute = FakeCursor.execute

            def record_execute(cursor, sql, args=()):
                updates.append((sql, args))
                original_execute(cursor, sql, args)

            FakeCursor.execute = record_execute
            try:
                row = {
                    "id": 1,
                    "stock_code": "MOCKC",
                    "stock_type": "C",
                    "ac_t_type": "C",
                    "qty": 10,
                    "ac_t_core_qty": 10,
                    "ac_t_state": ac.STATE_IDLE,
                    "ac_t_base_price": 100,
                    "ac_t_base_date": ac._today_la(),
                    "ac_t_open_date": ac._today_la(),
                    "ac_t_open_mode": "NORMAL",
                }

                result = ac.process_ac_t_symbol(conn, FakeClient(), row)
            finally:
                FakeCursor.execute = original_execute

            self.assertTrue(result.startswith("up_buy:10@101.50"))
            self.assertTrue(any("UP_T_HOLDING" in str(update) for update in updates))
        finally:
            ac.DRY_RUN = originals["DRY_RUN"]
            ac.get_latest_stock_price = originals["get_latest_stock_price"]
            ac._submit_limit_and_wait = originals["_submit_limit_and_wait"]

    def test_c_up_holding_sells_extra_lot_after_pullback(self):
        import app.strategy_ac_t as ac

        updates = []
        originals = {
            "DRY_RUN": ac.DRY_RUN,
            "get_latest_stock_price": ac.get_latest_stock_price,
            "_submit_limit_and_wait": ac._submit_limit_and_wait,
        }
        try:
            ac.DRY_RUN = True
            ac.get_latest_stock_price = lambda _symbol: 102.8
            ac._submit_limit_and_wait = (
                lambda _client, symbol, qty, side, price: ac.FillResult(
                    True, f"mock-{side.lower()}-{symbol}", "filled", qty, price
                )
            )

            conn = FakeConn()
            original_execute = FakeCursor.execute

            def record_execute(cursor, sql, args=()):
                updates.append((sql, args))
                original_execute(cursor, sql, args)

            FakeCursor.execute = record_execute
            try:
                row = {
                    "id": 1,
                    "stock_code": "MOCKC",
                    "stock_type": "C",
                    "ac_t_type": "C",
                    "qty": 20,
                    "ac_t_core_qty": 10,
                    "ac_t_state": ac.STATE_UP_HOLDING,
                    "ac_t_qty": 10,
                    "ac_t_buy_price": 101.5,
                    "ac_t_high_price": 104,
                    "ac_t_entry_time": ac._now_la().replace(tzinfo=None) - timedelta(minutes=60),
                }

                result = ac.process_ac_t_symbol(conn, FakeClient(), row)
            finally:
                FakeCursor.execute = original_execute

            self.assertTrue(result.startswith("up_sell:10@102.80"))
            self.assertTrue(any("UP_SELL" in str(update) for update in updates))
            self.assertTrue(any("IDLE" in str(update) for update in updates))
        finally:
            ac.DRY_RUN = originals["DRY_RUN"]
            ac.get_latest_stock_price = originals["get_latest_stock_price"]
            ac._submit_limit_and_wait = originals["_submit_limit_and_wait"]

    def test_c_loader_defaults_to_c_group_and_supports_symbol_filter(self):
        import app.strategy_ac_t as ac

        conn = FakeConn(fetchall_result=[{"stock_code": "MOCKC", "ac_t_type": "C"}])
        rows = ac.load_ac_t_rows(conn)
        self.assertEqual([{"stock_code": "MOCKC", "ac_t_type": "C"}], rows)
        self.assertEqual(("C",), conn.executed[-1][1])
        self.assertIn("NOT REGEXP", conn.executed[-1][0])

        conn = FakeConn(fetchall_result=[])
        ac.load_ac_t_rows(conn, symbol="mockc", group="C")
        self.assertEqual(("C", "MOCKC"), conn.executed[-1][1])

    def test_c_state_machine_skips_occ_option_symbols(self):
        import app.strategy_ac_t as ac

        row = {
            "id": 1,
            "stock_code": "MX260918C00007500",
            "stock_type": "C",
            "ac_t_type": "C",
            "qty": 1,
            "ac_t_core_qty": 1,
            "ac_t_state": ac.STATE_IDLE,
        }

        self.assertEqual("skip:option_symbol", ac.process_ac_t_symbol(FakeConn(), FakeClient(), row))


if __name__ == "__main__":
    unittest.main()
