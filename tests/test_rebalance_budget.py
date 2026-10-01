import unittest
from types import SimpleNamespace as NS
from unittest.mock import patch
from ultimate_v1.rebalance_budget import buying_budget
from ultimate_v1 import exposure_manager as em

class RebalanceBudgetTests(unittest.TestCase):
    def allocation(self, power=1000):
        return NS(pool_brokers=dict.fromkeys('BCD','trading'),
                  broker_snapshots={'trading':{'buying_power':power}},
                  pool_enabled=dict.fromkeys('BCD',True),available={'B':400,'C':584,'D':300})

    def test_locked_pools_not_counted(self):
        result=buying_budget(self.allocation(),NS(block_b=True,block_d=True))
        self.assertEqual(result['allowed_total'],584)
        self.assertEqual(result['executable_total'],0)

    def test_shared_power_counted_once(self):
        result=buying_budget(self.allocation(100),NS())
        self.assertAlmostEqual(result['allowed_total'],100)

    def test_global_block_and_missing_account_fail_closed(self):
        self.assertEqual(buying_budget(self.allocation(),NS(block_all_new=True))['allowed_total'],0)
        a=self.allocation();a.broker_snapshots={}
        self.assertEqual(buying_budget(a,NS())['allowed_total'],0)

    def test_auto_no_longer_bypasses_strategy_execution(self):
        with patch.object(em,'_submit_stock_order') as submit:
            result=em.execute_exposure_plan(NS(mode='AUTO',actions=[{'side':'sell','strategy_group':'B'}]))
        submit.assert_not_called()
        self.assertEqual(result[0]['status'],'skipped')
        self.assertIn('strategy_executor_required',result[0]['reason'])

    def test_buy_plan_uses_pool_budget_and_blocks_b_d(self):
        a=self.allocation();a.target_for=lambda group: a.available[group]
        with patch('ultimate_v1.capital_manager.get_capital_allocation',return_value=a),patch.object(em,'_strategy_weights',return_value={'B':.4,'C':.4,'D':.2}),patch.object(em,'_symbols_for_group',return_value=['QQQ']):
            _,actions=em._build_weighted_buy_actions(round_id='test',risk=NS(block_b=True,block_d=True),holdings=[],target_value=1400,current_value=0,target_pct=1.4,min_trade=100,reason='test')
        self.assertEqual(len(actions),1)
        self.assertEqual(actions[0]['strategy_group'],'C')
        self.assertEqual(actions[0]['delta_value'],584)
