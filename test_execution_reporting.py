import ast
import unittest
from pathlib import Path
from types import SimpleNamespace


class Connection:
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def execute(self, *args): return self
    def fetchone(self): return {'user_id': 1}
    def commit(self): pass


class ReportingTests(unittest.TestCase):
    def report(self, status):
        tree = ast.parse(Path(__file__).with_name('server.py').read_text())
        fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'autotrader_event')
        fn.decorator_list = []
        calls = []
        namespace = dict(Request=object, re=__import__('re'), AUTOTRADER_USER_ID=1,
            _authenticate_autotrader=lambda request: None, db=Connection,
            _notify=lambda *args: calls.append(args),
            HTTPException=lambda *args: ValueError(args))
        exec(compile(ast.Module(body=[fn], type_ignores=[]), 'reporting', 'exec'), namespace)
        namespace['autotrader_event'](SimpleNamespace(), {'signal_id':'rc1-123', 'status':status})
        return calls[0][3]

    def test_new_closing_fill_is_accepted_without_profit_claim(self):
        self.assertEqual(self.report('closing_fill'), 'Demo closing fill confirmed')

    def test_legacy_closing_fill_does_not_claim_profit(self):
        self.assertEqual(self.report('partial_profit'), 'Demo closing fill confirmed')
