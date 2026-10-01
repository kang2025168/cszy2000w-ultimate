import json
import unittest
from datetime import datetime, timedelta, timezone, date
from types import SimpleNamespace as NS
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo
from ultimate_v1 import d_grid as d
from ultimate_v1.b_holding_exit import time_exit_due, confirmed_entry
from ultimate_v1.alpaca_gateway import StockQuote

class HoldingRules(unittest.TestCase):
    def test_b_third_session_and_early_close(self):
        ny=ZoneInfo('America/New_York')
        dates=[date(2026,11,25), date(2026,11,27), date(2026,11,30)]
        sessions=[NS(date=x,open=datetime.combine(x,datetime.min.time().replace(hour=9,minute=30),ny),close=datetime.combine(x,datetime.min.time().replace(hour=13 if x.day==27 else 16),ny)) for x in dates]
        entry=datetime(2026,11,25,10,tzinfo=ny)
        self.assertFalse(time_exit_due(entry,.04,datetime(2026,11,30,15,49,tzinfo=ny),sessions))
        self.assertTrue(time_exit_due(entry,.04,datetime(2026,11,30,15,50,tzinfo=ny),sessions))
        self.assertFalse(time_exit_due(entry,.05,datetime(2026,11,30,15,50,tzinfo=ny),sessions))
        self.assertFalse(time_exit_due(entry,.04,datetime(2026,11,27,12,50,tzinfo=ny),sessions))
        # A third session with an early close uses that actual close.
        sessions=[NS(date=date(2026,11,24),open=datetime(2026,11,24,9,30,tzinfo=ny),close=datetime(2026,11,24,16,tzinfo=ny))]+sessions[:2]
        self.assertTrue(time_exit_due(datetime(2026,11,24,10,tzinfo=ny),.04,datetime(2026,11,27,12,50,tzinfo=ny),sessions))

    def test_sync_does_not_become_buy_date(self):
        client=MagicMock()
        self.assertIsNone(confirmed_entry(client,{'qty':10,'last_order_side':'sync','last_order_time':'2026-09-30'},[]))
        client.get_order_by_id.assert_not_called()
        client.get_order_by_id.return_value=NS(side='buy',filled_qty=10,filled_at=datetime(2026,9,25,15,tzinfo=timezone.utc))
        got=confirmed_entry(client,{'qty':10,'last_order_side':'sync'},[{'order_id':'b1'}])
        self.assertEqual(25,got.day)

    def test_day_guard_cooldown_ban_limit_and_reset(self):
        now=datetime(2026,9,30,16,tzinfo=timezone.utc)
        guard={'stops':{'b1':{'symbol':'X'}},'last_stop':now.timestamp()}
        with patch.object(d,'get_app_setting',side_effect=lambda key,default:json.dumps(guard) if key.endswith('2026-09-30') else default):
            self.assertEqual('stopped_symbol_today',d._entry_guard('X',now))
            self.assertEqual('stop_cooldown',d._entry_guard('Y',now+timedelta(seconds=599)))
            self.assertEqual('',d._entry_guard('Y',now+timedelta(seconds=600)))
            guard['stops']['b2']={'symbol':'Y'}
            self.assertEqual('daily_stop_limit',d._entry_guard('Z',now+timedelta(hours=1)))
            self.assertEqual('',d._entry_guard('X',now+timedelta(days=1)))

    def test_same_stop_is_counted_once(self):
        guard={}; cur=MagicMock()
        def save(sql,args):
            guard.clear();guard.update(json.loads(args[1]))
        cur.execute.side_effect=save
        with patch.object(d,'_day_guard',side_effect=lambda now=None:(dict(guard),'2026-09-30')):
            for _ in range(2):
                d._record_cycle_guard(cur,{'symbol':'X'},{'buy_order_id':'b1'},True)
        self.assertEqual(1,len(guard['stops']))

    def test_stale_bid_does_not_hide_fresh_trade(self):
        now=datetime.now(timezone.utc)
        q=StockQuote('X',98,101,102,now-timedelta(minutes=2),now,now-timedelta(minutes=2),True)
        self.assertTrue(d._stop_threshold_hit(100,q,now))
        q.trade_timestamp=now-timedelta(minutes=2)
        self.assertFalse(d._stop_threshold_hit(100,q,now))

    def test_stop_sells_only_remaining_d_lot(self):
        client=MagicMock()
        client.get_order_by_id.return_value=NS(status='canceled',filled_qty=2,filled_avg_price=101)
        client.get_open_position.return_value=NS(qty=100) # B/C share this symbol.
        cycle={'symbol':'X','sell_order_id':'s1','buy_filled_qty':10,'buy_filled_price':100}
        with patch.object(d,'_cycle_sell_totals',return_value=(2,202)),patch.object(d,'_submit_sell',return_value='submitted') as sell:
            d._last_minute_exit(MagicMock(),{'symbol':'X'},cycle,client)
        self.assertEqual(8,sell.call_args.args[3])
        self.assertTrue(sell.call_args.kwargs['force_market'])

    def test_old_signal_cannot_start_new_cycle(self):
        from ultimate_v1.d_entry_filter import check_entry
        now=datetime.now(timezone.utc)
        obs={'price':104,'previous_close':100,'quote_epoch':now.timestamp(),'samples':[(now.timestamp()-(19-i)*15,103+i*.05) for i in range(20)]}
        with patch('ultimate_v1.d_entry_filter.read_observation',return_value=obs):
            self.assertTrue(check_entry('X')['ok'])
            self.assertEqual('collecting_prices',check_entry('X',not_before=now.timestamp()-20)['reason'])

    def test_partial_buy_stop_cancels_then_sells_final_fill_qty(self):
        now=datetime.now(timezone.utc)
        quote=StockQuote('X',98,98,98,now)
        client=MagicMock()
        cycle={'symbol':'X','cycle_no':1,'buy_order_id':'buy1'}
        config={'symbol':'X'}
        with patch.object(d,'_stop_pending',return_value=False),patch.object(d,'set_app_setting'),patch.object(d,'_event'),patch.object(d,'_submit_sell') as sell:
            client.get_order_by_id.return_value=NS(status='partially_filled',filled_qty=3,filled_avg_price=100)
            self.assertEqual('stop_buy_cancel_pending',d._advance_buy(MagicMock(),config,cycle,quote,False,client))
            client.cancel_order_by_id.assert_called_once_with('buy1')
            sell.assert_not_called()
        with patch.object(d,'_stop_pending',return_value=True),patch.object(d,'_submit_sell',return_value='closing') as sell:
            client.get_order_by_id.return_value=NS(status='canceled',filled_qty=4,filled_avg_price=100)
            # Price recovering cannot cancel a latched stop.
            self.assertEqual('closing',d._advance_buy(MagicMock(),config,cycle,StockQuote('X',102,102,102,now),False,client))
            self.assertEqual(4,sell.call_args.args[3])
            self.assertTrue(sell.call_args.kwargs['force_market'])
