import unittest
from types import SimpleNamespace
from ultimate_v1.option_tracking import signed_value, validate

class OptionTrackingTests(unittest.TestCase):
    def test_debit_and_credit_pnl_use_leg_signs_and_multiplier(self):
        quotes={'low':SimpleNamespace(bid=5,ask=5.2),'high':SimpleNamespace(bid=2,ask=2.2)}
        legs=[{'side':'BUY','option_symbol':'low'},{'side':'SELL','option_symbol':'high'}]
        entry=signed_value(legs,quotes,True)
        close=signed_value(legs,quotes)
        self.assertAlmostEqual(entry,3.2)
        self.assertAlmostEqual((close-entry)*100*2,-80)
        reverse=[dict(l,side='SELL' if l['side']=='BUY' else 'BUY') for l in legs]
        self.assertAlmostEqual(signed_value(reverse,quotes,True),-2.8)
        self.assertAlmostEqual((signed_value(reverse,quotes)-signed_value(reverse,quotes,True))*100,-40)

    def test_invalid_quote_is_not_a_zero_value(self):
        with self.assertRaises(ValueError):
            signed_value([{'side':'BUY','option_symbol':'missing'}],{})

    def test_validates_exact_contracts_and_integer_quantity(self):
        payload=dict(symbol='QQQ',mode='BULL_CALL',expiry='2099-10-09',qty=1,row={'buy':{'option_symbol':'QQQ991009C00740000'},'sell':{'option_symbol':'QQQ991009C00750000'}})
        self.assertEqual(validate(payload)['legs'][0]['strike'],740)
        payload['qty']=1.5
        with self.assertRaises(ValueError): validate(payload)
        payload['qty']=1
        payload['mode']='BEAR_CALL'
        with self.assertRaises(ValueError): validate(payload)
