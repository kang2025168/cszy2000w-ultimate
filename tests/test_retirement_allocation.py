import unittest
from unittest.mock import patch

from ultimate_v1.retirement_allocation import DEFAULT_ITEMS, validate_config, load_config, target_budgets
from ultimate_v1.monthly_investment import _load_targets


def defaults():
    return {'items':[dict(symbol=s,percent=p,sleeve=g,label=l) for s,p,g,l in DEFAULT_ITEMS]}


class RetirementAllocationTests(unittest.TestCase):
    def test_default_split_and_c_membership(self):
        from ultimate_v1.strategy_c_watchlist import STRATEGY_C_WATCHLIST
        config = validate_config(defaults())
        self.assertEqual(100, sum(r['percent'] for r in config['items']))
        self.assertEqual(50, sum(r['percent'] for r in config['items'] if r['sleeve']=='theme'))
        self.assertTrue({r['symbol'] for r in config['items']} <= {r.symbol for r in STRATEGY_C_WATCHLIST})

    def test_manual_addition_persists_after_reload(self):
        import json
        config = defaults()
        config['items'][3]['percent'] -= 3
        config['items'].append(dict(symbol='GOOGL',percent=3,label='AI'))
        valid = validate_config(config)
        with patch('ultimate_v1.retirement_allocation.get_app_setting', return_value=json.dumps(valid)):
            self.assertEqual(valid, load_config())
            self.assertEqual(9, len(_load_targets('A',20)))
            with self.assertRaises(ValueError):
                _load_targets('A',8)

    def test_reject_bad_weights_and_duplicates(self):
        for value in [float('nan'),float('inf'),-1,0,16]:
            config = defaults(); config['items'][3]['percent']=value
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate_config(config)
        config = defaults(); config['items'][3]['symbol']='QQQ'
        with self.assertRaises(ValueError): validate_config(config)
        config = defaults(); config['items'][0]['percent']=21
        with self.assertRaises(ValueError): validate_config(config)

    def test_new_cash_obeys_target_split(self):
        rows = target_budgets(validate_config(defaults())['items'],10000,10000,{})
        self.assertEqual(5000, sum(r['buy_budget'] for r in rows if r['sleeve']=='fund'))
        self.assertEqual(5000, sum(r['buy_budget'] for r in rows if r['sleeve']=='theme'))

    def test_existing_overweight_funds_not_bought_or_sold(self):
        rows = target_budgets(validate_config(defaults())['items'],10000,1000,{'QQQ':4000,'VOO':4000,'XLV':1000})
        self.assertEqual(0, sum(r['buy_budget'] for r in rows if r['sleeve']=='fund'))
        self.assertAlmostEqual(1000, sum(r['buy_budget'] for r in rows))
        self.assertTrue(all(r['buy_budget'] >= 0 for r in rows))

    def test_no_borrowing_or_negative_budgets(self):
        rows = target_budgets(validate_config(defaults())['items'],10000,-100,{})
        self.assertEqual(0, sum(r['buy_budget'] for r in rows))
        with self.assertRaises(ValueError): target_budgets([],float('nan'),100,{})

    def test_monthly_preview_uses_retirement_account_and_never_submits(self):
        from types import SimpleNamespace
        from unittest.mock import Mock
        from ultimate_v1.monthly_investment import _plan_group
        client = Mock()
        client.get_account.return_value = SimpleNamespace(equity=10000, cash=1000)
        client.get_all_positions.return_value = [SimpleNamespace(symbol='QQQ',market_value=4000), SimpleNamespace(symbol='VOO',market_value=4000), SimpleNamespace(symbol='XLV',market_value=1000)]
        with patch('ultimate_v1.retirement_allocation.get_app_setting', return_value=''), patch('ultimate_v1.monthly_investment.get_capital_allocation',return_value=SimpleNamespace(available={'A':1000})), patch('ultimate_v1.monthly_investment.alpaca_gateway.trading_client',return_value=client) as factory, patch('ultimate_v1.monthly_investment.alpaca_gateway.get_latest_stock_price',return_value=10), patch('ultimate_v1.monthly_investment.fetch_all',return_value=[{'can_buy':1}]):
            result = _plan_group('A',{'budget_fraction':1,'max_symbols':20},execute=False)
        self.assertTrue(result['ok'])
        self.assertEqual(8,len(result['orders']))
        self.assertEqual(0,sum(r['qty'] for r in result['orders'][:3]))
        self.assertEqual(1000,sum(r['target_notional'] for r in result['orders']))
        factory.assert_called_once_with(pool='A')
        client.submit_order.assert_not_called()
