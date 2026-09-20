from __future__ import annotations

import unittest
from unittest.mock import patch

import ultimate_v1.bot_supervisor as supervisor


class BotSupervisorTests(unittest.TestCase):
    def setUp(self):
        supervisor._PROCESSES.clear()
        supervisor._RESTART_AFTER.clear()
        supervisor._RESTART_FAILURES.clear()

    def test_reconcile_restarts_enabled_missing_process(self):
        controls = [{"bot_name": name, "enabled": int(name == "risk_bot")} for name in supervisor.managed_bot_names()]
        started = []

        def fake_start(name):
            started.append(name)
            return True

        with patch.object(supervisor, "bot_controls", return_value=controls), patch.object(
            supervisor, "start_bot", side_effect=fake_start
        ):
            supervisor.reconcile_processes()

        self.assertEqual(["risk_bot"], started)

    def test_failed_restart_gets_backoff(self):
        controls = [{"bot_name": name, "enabled": int(name == "risk_bot")} for name in supervisor.managed_bot_names()]
        with patch.object(supervisor, "bot_controls", return_value=controls), patch.object(
            supervisor, "start_bot", return_value=False
        ):
            supervisor.reconcile_processes()

        self.assertEqual(1, supervisor._RESTART_FAILURES["risk_bot"])
        self.assertGreater(supervisor._RESTART_AFTER["risk_bot"], 0)

    def test_external_bot_switch_does_not_spawn_duplicate_child(self):
        with patch.dict(supervisor.environ, {"ULTIMATE_EXTERNAL_BOTS": "b_buy_bot,b_sell_bot"}), patch.object(
            supervisor, "set_bot_enabled"
        ), patch.object(supervisor, "heartbeat"), patch.object(supervisor, "start_bot") as start:
            self.assertTrue(supervisor.set_bot_runtime("b_buy_bot", True))

        start.assert_not_called()
