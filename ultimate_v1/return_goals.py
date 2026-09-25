"""Calendar return goals, settled once after each observed period ends."""
import json
import math
from datetime import date
from .db import db_conn


def curve_return(rows):
    if not rows:
        return None
    first = float(rows[0].get('equity') or rows[0].get('portfolio_value') or 0)
    last = float(rows[-1].get('equity') or rows[-1].get('portfolio_value') or 0)
    return (last-first)/first if first > 0 and math.isfinite(first) and math.isfinite(last) else None


def settle_goals(period, curve, target):
    prefix = f'RETURN_GOAL_V3:{period}:'
    key = prefix + curve['start_date']
    record = dict(start=curve['start_date'], end=curve['end_date'], target=target, status='pending')
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute('INSERT IGNORE INTO app_settings (setting_key,setting_value,updated_at) VALUES (%s,%s,NOW())', (key,json.dumps(record)))
            cur.execute('SELECT setting_key,setting_value FROM app_settings WHERE LEFT(setting_key,%s)=%s FOR UPDATE', (len(prefix),prefix))
            records = cur.fetchall()
            success = failure = 0
            for row in records:
                item = json.loads(row['setting_value'])
                if item['status'] == 'pending' and item['end'] < date.today().isoformat():
                    from .adjusted_returns import curve as adjusted_curve
                    history = adjusted_curve(period, (date.fromisoformat(item['start']), date.fromisoformat(item['end'])), refresh=False)
                    snapshots = history['rows']
                    # Require a recent endpoint; stale data cannot settle a period.
                    from datetime import timedelta
                    result = history['return_fraction'] if len(snapshots) >= 2 and snapshots[-1]['created_at'].date() >= date.fromisoformat(item['end']) - timedelta(days=3) else None
                    # Missing history is not a trading failure; leave it pending.
                    if result is not None:
                        item.update(status='success' if result >= item['target']-1e-10 else 'failure', result=result)
                        cur.execute('UPDATE app_settings SET setting_value=%s,updated_at=NOW() WHERE setting_key=%s', (json.dumps(item),row['setting_key']))
                success += item['status'] == 'success'
                failure += item['status'] == 'failure'
    return success, failure
