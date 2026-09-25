import ast
from pathlib import Path
import unittest


class EarlyLockTests(unittest.TestCase):
    def test_strict_threshold_and_never_lower_existing_stop(self):
        tree = ast.parse(Path('app/strategy_b.py').read_text())
        fn = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == '_calc_dynamic_trail_sl')
        ns = {'_safe_float':lambda v,d=0:float(v or d), 'TRAIL_LOCK_START_PCT':.05, 'TRAIL_LOCK_SL_MULT':1.0}
        exec(compile(ast.Module(body=[fn],type_ignores=[]),'trail','exec'),ns)
        calc=ns['_calc_dynamic_trail_sl']
        self.assertEqual(95,calc(100,103,95))
        self.assertEqual(101,calc(100,103.01,95))
        self.assertEqual(101,calc(100,106,101))
        self.assertEqual(104,calc(100,106,104))
        self.assertEqual(101,calc(100,100.5,101))
