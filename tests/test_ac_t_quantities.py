"""Quantity accounting at the A/C closing-leg boundary; no broker connections."""
import unittest
from unittest.mock import Mock, patch
from app import strategy_ac_t as ac


class ACTQuantityTests(unittest.TestCase):
    def check_close(self, group, direction, filled):
        row = dict(stock_code='MOCK', stock_type=group, ac_t_type=group,
                   ac_t_core_qty=100, ac_t_qty=20, qty=120 if direction=='up' else 80,
                   ac_t_buy_price=10, ac_t_sell_price=12)
        helper = '_sell_t_qty' if direction=='up' else '_buy_t_qty'
        finish = ac._finish_up_sell if direction=='up' else ac._finish_down_buy
        with patch.object(ac,'_min_leg_hold_ok',return_value=(True,'')), patch.object(ac,helper,return_value=ac.FillResult(True,'mock-order','filled' if filled==20 else 'canceled',filled,11)) as submit, patch.object(ac,'_record_t_result'), patch.object(ac,'_set_row') as update:
            finish(Mock(),Mock(),row,11,'TEST')
        self.assertEqual(20,submit.call_args.args[3])
        if filled==0:
            update.assert_not_called()
        else:
            fields=update.call_args.args[2]
            self.assertEqual(20-filled,fields['ac_t_qty'])
            self.assertEqual(100+(20-filled)*(1 if direction=='up' else -1),fields['qty'])
            if filled==20: self.assertEqual(ac.STATE_IDLE,fields['ac_t_state'])

    def test_buy_then_sell_full(self):
        for group in ('A','C'):
            with self.subTest(group=group): self.check_close(group,'up',20)

    def test_sell_then_buy_full(self):
        for group in ('A','C'):
            with self.subTest(group=group): self.check_close(group,'down',20)

    def test_buy_then_sell_partial(self):
        for group in ('A','C'):
            with self.subTest(group=group): self.check_close(group,'up',7)

    def test_sell_then_buy_partial(self):
        for group in ('A','C'):
            with self.subTest(group=group): self.check_close(group,'down',7)

    def test_no_fill_does_not_change_qty(self):
        for group in ('A','C'):
            for direction in ('up','down'):
                with self.subTest(group=group,direction=direction): self.check_close(group,direction,0)

    def test_in_progress_cycle_retains_saved_core(self):
        for group in ('A','C'):
            row=dict(stock_type=group,ac_t_type=group,ac_t_state=ac.STATE_DOWN_WAIT_BUY,ac_t_core_qty=100,qty=80)
            self.assertEqual(100,ac._core_qty(Mock(),row,80))
