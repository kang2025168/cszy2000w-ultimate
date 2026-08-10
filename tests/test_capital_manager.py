from __future__ import annotations

import unittest
from types import SimpleNamespace


class CapitalManagerTests(unittest.TestCase):
    def assertDictAlmostEqual(self, expected, actual):
        self.assertEqual(set(expected), set(actual))
        for key, value in expected.items():
            self.assertAlmostEqual(value, actual[key])

    def test_fallback_mode_weights_keep_d_intraday_allowance(self):
        import ultimate_v1.capital_manager as cm

        original_get_risk_state = cm.get_risk_state
        try:
            cm.get_risk_state = lambda: (_ for _ in ()).throw(RuntimeError("offline"))
            weights, allow_d = cm._mode_weights("NORMAL")
        finally:
            cm.get_risk_state = original_get_risk_state

        self.assertEqual({"A": 0.20, "B": 0.30, "C": 0.50, "D": 0.30}, weights)
        self.assertTrue(allow_d)

    def test_auto_margin_usage_steps_with_market_condition(self):
        import ultimate_v1.capital_manager as cm

        strong = SimpleNamespace(
            market_trend="向上",
            vix=14.5,
            qqq_change_pct=0.8,
            loss_days=0,
            max_drawdown=0.02,
            block_all=False,
        )
        middle = SimpleNamespace(
            market_trend="横盘",
            vix=17.0,
            qqq_change_pct=0.1,
            loss_days=0,
            max_drawdown=0.03,
            block_all=False,
            risk_preference="中性",
        )
        conservative = SimpleNamespace(
            market_trend="横盘",
            vix=14.0,
            qqq_change_pct=0.1,
            loss_days=0,
            max_drawdown=0.03,
            block_all=False,
            risk_preference="保守",
        )
        weak = SimpleNamespace(
            market_trend="向下",
            vix=29.0,
            qqq_change_pct=-1.0,
            loss_days=2,
            max_drawdown=0.12,
            block_all=False,
            risk_preference="激进",
        )
        strong.risk_preference = "激进"

        self.assertEqual(1.5, cm._auto_margin_usage_pct(strong)[0])
        self.assertEqual(1.2, cm._auto_margin_usage_pct(middle)[0])
        self.assertEqual(1.1, cm._auto_margin_usage_pct(conservative)[0])
        self.assertEqual(1.0, cm._auto_margin_usage_pct(weak)[0])

    def test_market_exposure_prefers_risk_recommended_exposure(self):
        import ultimate_v1.capital_manager as cm

        risk = SimpleNamespace(market_trend="横盘", vix=14.0, recommended_exposure=0.7)

        self.assertEqual(0.7, cm._market_exposure_pct(risk))

    def test_pool_base_percents_follow_base_ratio_and_transfer_rules(self):
        import os
        import ultimate_v1.capital_manager as cm

        keys = [
            "A_ACCOUNT_CAPITAL_PCT",
            "B_ACCOUNT_CAPITAL_PCT",
            "C_ACCOUNT_CAPITAL_PCT",
            "D_ACCOUNT_CAPITAL_PCT",
            "RISK_A_POOL_ENABLED",
            "RISK_B_POOL_ENABLED",
            "RISK_C_POOL_ENABLED",
            "RISK_D_POOL_ENABLED",
        ]
        old_env = {key: os.environ.get(key) for key in keys}
        original_get_app_setting = cm.get_app_setting
        try:
            for key in keys:
                os.environ.pop(key, None)

            def fake_get_app_setting(key, default=""):
                values = {
                    "A_ACCOUNT_CAPITAL_PCT": "0.2",
                    "B_ACCOUNT_CAPITAL_PCT": "0.5",
                    "C_ACCOUNT_CAPITAL_PCT": "0.2",
                    "D_ACCOUNT_CAPITAL_PCT": "0.1",
                    **fake_get_app_setting.flags,
                }
                return values.get(key, default)

            fake_get_app_setting.flags = {
                "RISK_A_POOL_ENABLED": "1",
                "RISK_B_POOL_ENABLED": "1",
                "RISK_C_POOL_ENABLED": "1",
                "RISK_D_POOL_ENABLED": "1",
            }

            cm.get_app_setting = fake_get_app_setting
            self.assertDictAlmostEqual({"A": 0.2, "B": 0.5, "C": 0.2, "D": 0.1}, cm._pool_base_percents())

            fake_get_app_setting.flags = {
                    "RISK_A_POOL_ENABLED": "0",
                    "RISK_B_POOL_ENABLED": "1",
                    "RISK_C_POOL_ENABLED": "1",
                    "RISK_D_POOL_ENABLED": "1",
            }
            self.assertDictAlmostEqual({"A": 0.0, "B": 0.7, "C": 0.2, "D": 0.1}, cm._pool_base_percents())

            fake_get_app_setting.flags = {
                    "RISK_A_POOL_ENABLED": "1",
                    "RISK_B_POOL_ENABLED": "1",
                    "RISK_C_POOL_ENABLED": "0",
                    "RISK_D_POOL_ENABLED": "1",
            }
            self.assertDictAlmostEqual({"A": 0.2, "B": 0.7, "C": 0.0, "D": 0.1}, cm._pool_base_percents())

            fake_get_app_setting.flags = {
                    "RISK_A_POOL_ENABLED": "1",
                    "RISK_B_POOL_ENABLED": "1",
                    "RISK_C_POOL_ENABLED": "1",
                    "RISK_D_POOL_ENABLED": "0",
            }
            self.assertDictAlmostEqual({"A": 0.2, "B": 0.5, "C": 0.3, "D": 0.0}, cm._pool_base_percents())

            fake_get_app_setting.flags = {
                    "RISK_A_POOL_ENABLED": "0",
                    "RISK_B_POOL_ENABLED": "1",
                    "RISK_C_POOL_ENABLED": "0",
                    "RISK_D_POOL_ENABLED": "0",
            }
            self.assertDictAlmostEqual({"A": 0.0, "B": 1.0, "C": 0.0, "D": 0.0}, cm._pool_base_percents())
        finally:
            cm.get_app_setting = original_get_app_setting
            for key, value in old_env.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value


if __name__ == "__main__":
    unittest.main()
