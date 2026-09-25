import unittest
from unittest.mock import MagicMock
from ultimate_v1.adjusted_returns import metrics, _activities

class AdjustedReturnsTests(unittest.TestCase):
    def test_deposit_withdrawal_and_nonpositive_basis(self):
        first={'equity':3000,'net_flow':0}
        self.assertAlmostEqual(metrics(first,{'equity':4100,'net_flow':1000})['return_fraction'],.025)
        self.assertAlmostEqual(metrics(first,{'equity':2100,'net_flow':-1000})['return_fraction'],.05)
        self.assertEqual(metrics(first,{'equity':4000,'net_flow':1000})['profit'],0)
        self.assertIsNone(metrics(first,{'equity':100,'net_flow':-3000})['return_fraction'])

    def test_signed_transfer_amounts(self):
        client=MagicMock()
        client.get.return_value=[{'id':'a','activity_type':'CSD','net_amount':'1000'}, {'id':'b','activity_type':'CSW','net_amount':'-200'}]
        self.assertEqual(sum(v for _,v in _activities(client,'2026-09-24')),800)
        client.get.return_value={'error':'unavailable'}
        with self.assertRaises(ValueError): _activities(client,'2026-09-24')

    def test_new_epoch_excludes_existing_transfers_and_deduplicates_accounts(self):
        from unittest.mock import patch
        from types import SimpleNamespace
        from ultimate_v1.adjusted_returns import _collect
        cursor=MagicMock()
        cursor.fetchone.return_value=None
        client=MagicMock()
        client.get_account.return_value=SimpleNamespace(id='same-account', equity='3000')
        client.get.return_value=[{'id':'old','activity_type':'CSD','net_amount':'3000'}]
        with patch('ultimate_v1.adjusted_returns.db_conn') as db, patch('ultimate_v1.account_config.profile_for_pool',side_effect=lambda p:p), patch('ultimate_v1.alpaca_gateway.trading_client',return_value=client):
            db.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value=cursor
            _collect()
            point=[c for c in cursor.execute.call_args_list if c.args[0].startswith('INSERT INTO adjusted_return_points')][0]
            self.assertEqual(point.args[1],(3000,0))
            client.get.assert_called_once()
