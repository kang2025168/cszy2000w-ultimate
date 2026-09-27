import unittest
from ultimate_v1.weekly_pools import calculate

class WeeklyPoolTests(unittest.TestCase):
    def snapshot(self, **changes):
        result = dict(equity=3000,flow=0,values={'B':800,'C':700,'D':0},cash={'B':0,'C':0,'D':0})
        result.update(changes)
        return result

    def test_profit_stays_with_owner_and_round_trip_is_counted_once(self):
        initial = self.snapshot()
        current = self.snapshot(values={'B':820,'C':600,'D':0}, cash={'B':0,'C':100,'D':30})
        r=calculate(initial,current)
        self.assertEqual((1220,1200,630),tuple(r[g]['equity'] for g in 'BCD'))
        self.assertEqual((20,0,30),tuple(r[g]['pnl'] for g in 'BCD'))

    def test_transfers_change_principal_not_profit(self):
        for flow in (500,-500):
            r=calculate(self.snapshot(),self.snapshot(flow=flow))
            self.assertEqual(0,sum(x['pnl'] for x in r.values()))
            self.assertEqual(3000+flow,sum(x['equity'] for x in r.values()))
            self.assertEqual(flow*.4,r['C']['net_flow'])

    def test_new_week_does_not_carry_old_profit_as_current_profit(self):
        initial=self.snapshot(equity=3100)
        r=calculate(initial,initial)
        self.assertEqual(1240,r['B']['initial'])
        self.assertEqual(0,r['B']['pnl'])

    def test_no_division_by_zero_after_withdrawal(self):
        r=calculate(self.snapshot(),self.snapshot(flow=-3000))
        self.assertIsNone(r['B']['return_pct'])

class ObservationTests(unittest.TestCase):
    def observe(self, holdings, fills, owners, previous=None):
        from types import SimpleNamespace as NS
        from unittest.mock import patch
        from ultimate_v1.weekly_pools import _observe
        class Cursor:
            def execute(self, sql, args=None):
                self.rows = holdings if 'FROM position_holdings' in sql else [dict(order_id=k,pool=v) for k,v in owners.items()] if sql == 'SELECT order_id,pool FROM weekly_pool_orders' else []
            def fetchall(self):
                return self.rows
        qty=sum(x['qty'] for x in holdings)
        client=NS(get_all_positions=lambda:[NS(symbol='META',qty=qty,current_price=100)],
                  get_account=lambda:NS(equity=3000,id='account'),
                  get_order_by_id=lambda _:NS(client_order_id='external'))
        with patch('ultimate_v1.weekly_pools._pages',return_value=fills), patch('ultimate_v1.adjusted_returns._activities',return_value=[]):
            return _observe(Cursor(),client,'2026-09-28',previous)

    def test_overlapping_symbol_profit_is_independent(self):
        holdings=[dict(symbol='META',strategy_group='B',qty=1),dict(symbol='META',strategy_group='C',qty=2)]
        initial=self.observe(holdings,[],{})
        fills=[dict(id='buy',order_id='b',symbol='META',side='buy',qty=1,price=90),
               dict(id='sell',order_id='s',symbol='META',side='sell',qty=1,price=110)]
        current=self.observe(holdings,fills,{'b':'B','s':'B'},initial)
        r=calculate(initial,current)
        self.assertEqual(20,r['B']['pnl'])
        self.assertEqual(0,r['C']['pnl'])

    def test_unknown_new_fill_is_not_guessed(self):
        initial=self.observe([],[],{})
        fill=dict(id='new',order_id='unknown',symbol='META',side='buy',qty=1,price=100)
        with self.assertRaises(ValueError):
            self.observe([], [fill], {}, initial)

    def test_old_fill_discovered_later_does_not_create_profit(self):
        fill=dict(id='old',order_id='b',symbol='META',side='buy',qty=1,price=90)
        holdings=[dict(symbol='META',strategy_group='B',qty=1)]
        initial=self.observe(holdings,[fill],{})
        current=self.observe(holdings,[fill],{'b':'B'},initial)
        self.assertEqual(0,calculate(initial,current)['B']['pnl'])
