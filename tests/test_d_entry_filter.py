import unittest
from datetime import datetime, timezone
from unittest.mock import Mock, patch
from ultimate_v1.d_entry_filter import evaluate_entry, check_entry

class DEntryFilterTests(unittest.TestCase):
    def setUp(self):
        self.now=datetime(2026,9,22,16,0,tzinfo=timezone.utc)
        self.bars=[(self.now.timestamp()-(19-i)*15,103+i*.05) for i in range(20)]

    def test_rising_above_three_passes(self):
        self.assertTrue(evaluate_entry(104,100,self.bars,now=self.now)['ok'])

    def test_exactly_three_and_below_fail(self):
        for price in (103,102):
            self.assertFalse(evaluate_entry(price,100,self.bars,now=self.now)['ok'])

    def test_positive_day_but_falling_fails(self):
        bars=[(t,106-i*.1) for i,(t,_) in enumerate(self.bars)]
        self.assertFalse(evaluate_entry(105,100,bars,now=self.now)['ok'])

    def test_pullback_fails(self):
        self.assertFalse(evaluate_entry(103.8,100,self.bars,now=self.now)['ok'])

    def test_stale_missing_gapped_and_flat_fail(self):
        for bars in (self.bars[:-2],[(t-600,p) for t,p in self.bars],
                     [(t,104) for t,p in self.bars], self.bars[:5]+self.bars[6:]):
            self.assertFalse(evaluate_entry(104,100,bars,now=self.now)['ok'])

    def test_network_failure_blocks(self):
        with patch('ultimate_v1.d_entry_filter.collect_prices',side_effect=TimeoutError):
            self.assertFalse(check_entry('MOCK',refresh=True)['ok'])

    def test_blocked_entry_does_not_submit(self):
        from ultimate_v1 import d_grid as d
        from ultimate_v1.alpaca_gateway import StockQuote
        client=Mock()
        with patch('ultimate_v1.d_entry_filter.check_entry',return_value={'ok':False,'reason':'not_rising'}), patch.object(d,'_submit_limit') as submit, patch.object(d,'_cycle_budget') as budget:
            result=d._start_cycle(Mock(),{'symbol':'MOCK','max_spread':1},{},StockQuote('MOCK',104,104,104),False,client)
        self.assertEqual('entry_filter:not_rising',result)
        submit.assert_not_called()
        budget.assert_not_called()

    def test_failed_entry_requests_immediate_reselection(self):
        from ultimate_v1 import d_grid as d
        with patch('ultimate_v1.d_entry_filter.sample_tick'), patch.object(d,'ensure_schema'), patch.object(d,'_auto_select_candidate',side_effect=[None,{'symbol':'NEXT'}]) as select, patch.object(d,'fetch_all',return_value=[{'symbol':'MOCK'}]), patch.object(d,'run_symbol',return_value='entry_filter:not_rising'):
            result=d.run_all()
        self.assertEqual({'symbol':'AUTO','ok':True,'message':"{'symbol': 'NEXT'}"},result[-1])
        self.assertEqual({'force':True,'exclude':{'MOCK'}},select.call_args.kwargs)

    def test_duplicate_trade_is_not_a_new_sample(self):
        from types import SimpleNamespace as NS
        from datetime import timedelta
        from ultimate_v1.d_entry_filter import update_observation
        snapshot=NS(latest_trade=NS(timestamp=self.now,price=104),previous_daily_bar=NS(timestamp=self.now-timedelta(days=1),close=100),daily_bar=NS(high=105,volume=5000000))
        first=update_observation({},snapshot,self.now,'iex')
        second=update_observation(first,snapshot,self.now+timedelta(seconds=15),'iex')
        self.assertEqual(1,len(second['samples']))
        snapshot.latest_trade.timestamp=self.now+timedelta(seconds=15)
        third=update_observation(second,snapshot,self.now+timedelta(seconds=15),'iex')
        self.assertEqual(2,len(third['samples']))

    def test_stale_trade_does_not_create_sample(self):
        from types import SimpleNamespace as NS
        from datetime import timedelta
        from ultimate_v1.d_entry_filter import update_observation
        snapshot=NS(latest_trade=NS(timestamp=self.now-timedelta(seconds=60),price=104))
        self.assertEqual({},update_observation({},snapshot,self.now,'iex'))

    def test_restart_uses_persisted_samples(self):
        observation=dict(price=104,previous_close=100,quote_epoch=self.now.timestamp(),samples=self.bars)
        with patch('ultimate_v1.d_entry_filter.read_observation',return_value=observation), patch('ultimate_v1.d_entry_filter.datetime') as clock:
            clock.now.return_value=self.now
            clock.fromtimestamp.side_effect=datetime.fromtimestamp
            self.assertTrue(check_entry('MOCK')['ok'])

    def test_single_last_spike_without_positive_slope_fails(self):
        prices=[105-i*.1 for i in range(19)]+[105]
        bars=[(t,p) for (t,_),p in zip(self.bars,prices)]
        self.assertFalse(evaluate_entry(105,100,bars,now=self.now)['ok'])

    def test_polling_jitter_still_collects_twenty_in_five_minutes(self):
        from types import SimpleNamespace as NS
        from datetime import timedelta
        from ultimate_v1.d_entry_filter import update_observation
        old={}
        for i in range(20):
            now=self.now-timedelta(seconds=(19-i)*15-(i%2)*2)
            snapshot=NS(latest_trade=NS(timestamp=now,price=103+i*.05),previous_daily_bar=NS(timestamp=now-timedelta(days=1),close=100),daily_bar=NS(high=105,volume=5000000))
            old=update_observation(old,snapshot,now,'iex')
        self.assertEqual(20,len(old['samples']))
        self.assertTrue(evaluate_entry(104,100,old['samples'],now=now)['ok'])
