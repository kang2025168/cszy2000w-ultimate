import unittest
from datetime import date, datetime
from unittest.mock import patch, MagicMock
from ultimate_v1 import adjusted_returns as a

class EpochResetTests(unittest.TestCase):
    def test_calendar_axes(self):
        with patch.object(a,'tracking_today',return_value=date(2026,9,26)):
            self.assertEqual(a.tracking_bounds('week'),(date(2026,9,28),date(2026,10,2)))
            self.assertEqual(a.tracking_bounds('month'),(date(2026,9,1),date(2026,9,30)))
            self.assertEqual(a.tracking_bounds('year'),(date(2026,1,1),date(2026,12,31)))
            self.assertEqual(a.tracking_bounds('all'),(date(2026,9,26),date(2026,9,26)))

    def test_old_points_excluded_baseline_preserved_and_flows_adjusted(self):
        points=[dict(id=1,created_at=datetime(2026,9,25),equity=2000,net_flow=0),
                dict(id=2,created_at=datetime(2026,9,26),equity=3000,net_flow=0),
                dict(id=3,created_at=datetime(2026,9,28,12),equity=4100,net_flow=1000)]
        cur=MagicMock();cur.fetchall.return_value=points
        cur.fetchone.return_value={'setting_value':'2'}
        with patch.object(a,'db_conn') as db, patch.object(a,'tracking_today',return_value=date(2026,9,28)):
            db.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value=cur
            result=a.curve('week',refresh=False)
        self.assertEqual(result['rows'][0]['id'],2)
        self.assertEqual(result['rows'][0]['snapshot_date'],'2026-09-28')
        self.assertEqual(result['end_date'],'2026-10-02')
        self.assertAlmostEqual(result['return_fraction'],.025)
