import unittest
from datetime import datetime,timezone,timedelta
from types import SimpleNamespace as NS
from unittest.mock import patch,Mock
from ultimate_v1 import daily_advice as a

class DailyAdviceTests(unittest.TestCase):
    def test_stale_quote_never_prioritized(self):
        now=datetime.now(timezone.utc)
        q=a.quote_summary(NS(latest_trade=NS(price=110,timestamp=now-timedelta(hours=1)),previous_daily_bar=NS(close=100)),now)
        c=dict(b=[dict(symbol='MOCK',quote=q,can_buy=1,trigger_price=100)],d=[],option_modes=[])
        self.assertEqual('等待',a.rule_report(c)['rows'][0]['decision'])

    def test_d_filter_block_wins_over_positive_gain(self):
        c=dict(b=[],d=[dict(symbol='MOCK',quote={'fresh':True,'price':110},entry_filter={'ok':False,'reason':'collecting_prices'})],option_modes=[])
        self.assertEqual('等待',a.rule_report(c)['rows'][0]['decision'])

    def test_b_trigger_is_watch_only_not_execution(self):
        c=dict(b=[dict(symbol='MOCK',quote={'fresh':True,'price':110},can_buy=1,trigger_price=100)],d=[],option_modes=[])
        self.assertEqual('优先观察',a.rule_report(c)['rows'][0]['decision'])

    def test_options_without_chain_decline_contract_advice(self):
        c=dict(b=[],d=[],option_modes=[dict(mode='BULL_CALL',label='看涨',desc='借方价差')])
        self.assertIn('暂不给出合约购买建议',a.rule_report(c)['option_guidance'][0]['advice'])

    def test_missing_ai_configuration_does_not_start(self):
        with patch('ultimate_v1.config.env_str',return_value=''):
            self.assertFalse(a.start('ai')['ok'])

    def test_ai_uses_read_only_responses_without_tools(self):
        response=Mock(status_code=200)
        response.json.return_value={'status':'completed','output':[{'type':'message','content':[{'type':'output_text','text':'等待新鲜行情'}]}]}
        with patch('ultimate_v1.config.env_str',side_effect=lambda k,d='':'test-key' if k=='OPENAI_API_KEY' else 'test-model'), patch('requests.post',return_value=response) as post:
            result=a.ai_report({'b':[],'d':[],'options':[]})
        self.assertEqual('等待新鲜行情',result['text'])
        args=post.call_args.kwargs
        self.assertFalse(args['json']['store'])
        self.assertNotIn('tools',args['json'])
        self.assertNotIn('test-key',args['json']['input'])

    def test_provider_error_is_sanitized(self):
        with patch('ultimate_v1.config.env_str',return_value='configured'),patch('requests.post',return_value=Mock(status_code=403,text='secret detail')):
            with self.assertRaisesRegex(RuntimeError,'HTTP 403') as caught: a.ai_report({})
        self.assertNotIn('secret',str(caught.exception))

    def test_duplicate_click_does_not_start_another_job(self):
        old=dict(a._state)
        try:
            a._state.update(status='running')
            with patch.object(a,'Thread') as thread:
                self.assertEqual('running',a.start('rules')['status'])
                thread.assert_not_called()
        finally:
            a._state.clear(); a._state.update(old)
