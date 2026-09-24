import unittest
from types import SimpleNamespace as NS
from unittest.mock import patch
from ultimate_v1 import capital_manager as cm, exposure_manager as em

class CapitalConsistencyTests(unittest.TestCase):
    def test_retirement_excluded_from_budget_ratios(self):
        a=NS(pool_brokers={'B':'trading'},broker_snapshots={'trading':{'equity':3117.69}},used={'A':5000,'B':1825.2,'C':1036.61,'D':0},target_for=lambda g:{'B':1870.62,'C':1870.62,'D':935.31}[g])
        r=cm.margin_budget_summary(a)
        self.assertAlmostEqual(91.8,r['exposure_percent']*100,places=1)
        self.assertAlmostEqual(61.2,r['budget_used_percent']*100,places=1)
        self.assertAlmostEqual(150,r['target_percent']*100,places=2)

    def test_target_shared_and_d_budget_not_redistributed(self):
        with patch.object(cm,'_pool_base_percents',return_value={'B':.4,'C':.4,'D':.2}),patch.object(cm,'_risk_percents',return_value=(1.5,{'B':1,'C':1,'D':1})),patch.dict('os.environ',{'REBALANCE_INCLUDE_D_BUY':'0'}):
            self.assertEqual(1.5,em._target_exposure_pct()[0])
            self.assertEqual({'B':.4,'C':.4},em._strategy_weights(NS()))

    def test_retirement_holding_not_used_for_margin_rebalance(self):
        with patch.object(em,'fetch_all',return_value=[dict(symbol='QQQ',strategy_group='A',qty=10,current_price=100,market_value=1000),dict(symbol='NVDA',strategy_group='C',qty=2,current_price=100,market_value=200)]):
            self.assertEqual(['C'],[r.strategy_group for r in em._load_open_holdings()])

    def test_account_failure_does_not_use_mixed_snapshot(self):
        with patch.object(em.alpaca_gateway,'get_account_snapshot',return_value=None):
            self.assertEqual(0,em._account_equity())
