"""Persisted retirement targets; saving a target never submits an order."""
from __future__ import annotations

import json
import math
import re

from .config import settings
from .db import db_conn
from .state_store import get_app_setting

CONFIG_KEY = 'RETIREMENT_ALLOCATION_V1'
DEFAULT_ITEMS = (
    ('QQQ', 20, 'fund', '纳斯达克 100'),
    ('VOO', 20, 'fund', '标普 500'),
    ('XLV', 10, 'fund', '医疗基金'),
    ('MSFT', 15, 'theme', 'AI 平台'),
    ('NVDA', 15, 'theme', 'AI 芯片'),
    ('ISRG', 10, 'theme', '医疗机器人'),
    ('TER', 5, 'theme', '工业机器人'),
    ('IBIT', 5, 'theme', '比特币 ETF'),
)


def validate_config(config: dict) -> dict:
    if not isinstance(config, dict) or not isinstance(config.get('items'), list):
        raise ValueError('配置必须包含标的列表')
    if not 4 <= len(config['items']) <= 100:
        raise ValueError('标的数量必须在 4 到 100 之间')
    items, seen = [], set()
    for row in config['items']:
        if not isinstance(row, dict):
            raise ValueError('标的格式错误')
        symbol = str(row.get('symbol', '')).strip().upper()
        if not re.fullmatch(r'[A-Z][A-Z0-9.\-]{0,14}', symbol) or symbol in seen:
            raise ValueError('股票代码无效或重复：' + symbol)
        seen.add(symbol)
        try:
            percent = float(row.get('percent'))
        except (ValueError, TypeError):
            raise ValueError('比例必须为数字') from None
        if not math.isfinite(percent) or not 0 < percent <= 50:
            raise ValueError('每个标的比例必须大于 0 且不超过 50%')
        sleeve = 'fund' if symbol in {'QQQ', 'VOO', 'XLV'} else 'theme'
        items.append(dict(symbol=symbol, percent=percent, sleeve=sleeve,
                          label=str(row.get('label') or symbol)[:60]))
    funds = {r['symbol']: r['percent'] for r in items if r['sleeve'] == 'fund'}
    if funds != {'QQQ': 20, 'VOO': 20, 'XLV': 10}:
        raise ValueError('基金固定为 QQQ 20%、VOO 20%、XLV 10%')
    if abs(sum(r['percent'] for r in items if r['sleeve'] == 'theme') - 50) > 0.000001:
        raise ValueError('主题标的比例合计必须等于 50%')
    return {'items': items, 'fund_percent': 50, 'theme_percent': 50}


def load_config() -> dict:
    raw = get_app_setting(CONFIG_KEY, '')
    if raw:
        return validate_config(json.loads(raw))
    return validate_config({'items': [dict(symbol=s, percent=p, sleeve=g, label=l)
                                     for s, p, g, l in DEFAULT_ITEMS]})


def apply_targets(cur, config: dict) -> None:
    table = settings().ops_table
    if not table.replace('_', '').isalnum():
        raise ValueError('Invalid operations table')
    # Removed targets remain visible as holdings, but no longer receive monthly buys.
    cur.execute(f"UPDATE `{table}` SET weight=0 WHERE UPPER(stock_type)='A'")
    for item in config['items']:
        cur.execute(f"""
            INSERT INTO `{table}` (stock_code,stock_type,weight,is_bought,can_buy,can_sell,qty,
                strategy_group,capital_pool,margin_used,ac_t_enabled,ac_t_type,ac_t_state,
                last_order_intent,created_at,updated_at)
            VALUES (%s,'A',%s,0,1,0,0,'A','A',0,0,'A','IDLE',%s,NOW(),NOW())
            ON DUPLICATE KEY UPDATE weight=VALUES(weight),strategy_group='A',capital_pool='A',
                margin_used=0,updated_at=NOW()
        """, (item['symbol'], item['percent'] / 100,
               f"A:TARGET {item['sleeve']} {item['percent']:g}%"))


def save_config(config: dict) -> dict:
    config = validate_config(config)
    # Configuration and strategy rows commit together, without changing held quantities.
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute('''INSERT INTO app_settings (setting_key,setting_value,updated_at)
                VALUES (%s,%s,NOW()) ON DUPLICATE KEY UPDATE
                setting_value=VALUES(setting_value),updated_at=NOW()''',
                (CONFIG_KEY, json.dumps(config, ensure_ascii=False)))
            apply_targets(cur, config)
    return config


def target_budgets(items: list[dict], equity: float, available: float, positions: dict) -> list[dict]:
    """Allocate new cash to deficits, never sell or borrow to reach target weights."""
    if not math.isfinite(equity) or equity < 0 or not math.isfinite(available):
        raise ValueError('账户资金无效')
    rows = []
    for item in items:
        current = float(positions.get(item['symbol'], 0))
        if not math.isfinite(current) or current < 0:
            raise ValueError('养老金持仓金额无效')
        target = equity * item['percent'] / 100
        rows.append({**item, 'portfolio_target': target, 'current_value': current,
                     'deficit': max(0, target - current)})
    total = sum(row['deficit'] for row in rows)
    scale = min(1, max(0, available) / total) if total else 0
    for row in rows:
        row['buy_budget'] = row['deficit'] * scale
    return rows
