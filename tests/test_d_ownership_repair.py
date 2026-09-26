import unittest
from types import SimpleNamespace
from unittest.mock import patch, MagicMock
from ultimate_v1 import sync_positions as s


class OwnershipRepairTests(unittest.TestCase):
    def run_repair(self, broker_qty):
        conn=MagicMock();cur=conn.__enter__.return_value.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value={'symbol':'META','cycle_no':6,'buy_order_id':'buy','sell_order_id':'sell'}
        cur.fetchall.return_value=[{'id':1,'stock_type':'C','strategy_group':'C'}]
        client=MagicMock()
        client.get_order_by_id.return_value=SimpleNamespace(client_order_id='dgrid-META-6-b',status='filled',filled_qty=1,filled_avg_price=777.46)
        with patch.object(s,'db_conn',return_value=conn),patch.object(s,'profile_for_pool',return_value='trading'),patch.object(s.alpaca_gateway,'trading_client',return_value=client),patch('ultimate_v1.d_grid._cycle_sell_totals',return_value=(0,0)):
            s._repair_verified_d_ownership([SimpleNamespace(symbol='META',qty=broker_qty)],'trading',{'B','C','D','F'})
        return cur

    def test_exact_d_lot_repairs_c_label(self):
        cur=self.run_repair(1)
        updates=[c.args[0] for c in cur.execute.call_args_list if c.args[0].startswith('UPDATE')]
        self.assertEqual(2,len(updates))
        self.assertIn("strategy_group='D'",updates[0])

    def test_mixed_broker_position_is_not_reassigned(self):
        cur = self.run_repair(2)
        self.assertFalse(any(c.args[0].startswith('UPDATE') for c in cur.execute.call_args_list))

    def test_partition_keeps_other_strategy_shares(self):
        conn=MagicMock(); cur=conn.__enter__.return_value.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value={'symbol':'META','cycle_no':6,'buy_order_id':'buy'}
        client=MagicMock()
        client.get_order_by_id.return_value=SimpleNamespace(client_order_id='dgrid-META-6-b',status='filled',filled_qty=3,filled_avg_price=100)
        positions=[SimpleNamespace(symbol='META',qty=12,avg_entry_price=100,current_price=105)]
        with patch.object(s,'db_conn',return_value=conn),patch.object(s,'profile_for_pool',return_value='trading'),patch.object(s.alpaca_gateway,'trading_client',return_value=client),patch('ultimate_v1.d_grid._cycle_sell_totals',return_value=(1,105)):
            own, other=s._partition_d_positions(positions,'trading',{'B','C','D','F'})
        self.assertEqual(own[0].qty,2)
        self.assertEqual(other[0].qty,10)
        self.assertEqual(positions[0].qty,12)
