import unittest
import time


def test_rc1_contract_preserves_armed_plan_and_rejects_stale_feed():
    import ast
    from pathlib import Path
    from datetime import datetime,timedelta,timezone
    tree=ast.parse(Path(__file__).with_name('server.py').read_text())
    fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_rc1_demo_plan')
    namespace=dict(datetime=datetime,timedelta=timedelta,timezone=timezone,time=time,
                   _entry_assessment=lambda result,sess:{'quality':'READY','inside_entry':True})
    exec(compile(ast.Module(body=[fn],type_ignores=[]),'rc1_plan','exec'),namespace)
    make=namespace['_rc1_demo_plan']
    sess=dict(id=123,status='ARMED',side='LONG',planned_entry_low=100,planned_entry_high=102,stop=95,tp1=110,tp2=115)
    result=dict(signal='LONG',ts=int(time.time()*1000),stop=50)
    p=make(sess,result,datetime.now(timezone.utc).isoformat())
    assert p['stop_price']==95 and p['tp1']==110 and p['tp2']==115
    assert make(sess,result,(datetime.now(timezone.utc)-timedelta(minutes=5)).isoformat()) is None
    result['signal']='SHORT'
    assert make(sess,result,datetime.now(timezone.utc).isoformat()) is None

class ContractTests(unittest.TestCase):
    def test_untraded_stale_plan_retires_without_manual_approval(self):
        import ast
        from pathlib import Path
        from datetime import datetime,timezone,timedelta
        tree=ast.parse(Path(__file__).with_name('server.py').read_text())
        fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_untraded_plan_invalidation')
        namespace=dict(datetime=datetime,timezone=timezone)
        exec(compile(ast.Module(body=[fn],type_ignores=[]),'invalidation','exec'),namespace)
        check=namespace['_untraded_plan_invalidation']
        sess=dict(side='SHORT',stop=52500,tp2=52300,strategy='scalp',updated_at=datetime.now(timezone.utc).isoformat())
        self.assertEqual(check(sess,dict(signal='SHORT',price=51100)), 'UNTRADED_TARGET_PASSED')
        self.assertEqual(check(sess,dict(signal='SHORT',price=52600)), 'UNTRADED_STOP_PASSED')
        self.assertEqual(check(sess,dict(signal='LONG',price=52400)), 'DIRECTION_CHANGED')
        self.assertIsNone(check(sess,dict(signal='SHORT',price=52400)))
        sess['updated_at']=(datetime.now(timezone.utc)-timedelta(hours=2)).isoformat()
        self.assertEqual(check(sess,dict(signal='SHORT',price=52400)), 'UNTRADED_HORIZON_EXPIRED')

    def test_current_armed_geometry(self):
        test_rc1_contract_preserves_armed_plan_and_rejects_stale_feed()

    def test_feed_candle_helper_survives_bridge_changes(self):
        import ast
        from pathlib import Path
        from typing import Any
        tree=ast.parse(Path(__file__).with_name('server.py').read_text())
        fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_one_minute_bar')
        namespace={'Any':Any}
        exec(compile(ast.Module(body=[fn],type_ignores=[]),'candle_helper','exec'),namespace)
        bar=namespace['_one_minute_bar']({'frames':[{'tf':'1m','o':100,'h':102,'l':99,'c':101}]})
        self.assertEqual(bar,{'o':100.0,'h':102.0,'l':99.0,'c':101.0})

    def test_private_helpers_called_by_feed_exist(self):
        import ast
        from pathlib import Path
        tree=ast.parse(Path(__file__).with_name('server.py').read_text())
        names={n.name for n in tree.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef,ast.ClassDef))}
        for node in tree.body:
            if isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef)) and node.name in ('_process_copilot','_ingest_tradingview_for_user'):
                for call in ast.walk(node):
                    if isinstance(call,ast.Call) and isinstance(call.func,ast.Name) and call.func.id.startswith('_'):
                        self.assertIn(call.func.id,names)

if __name__ == "__main__":
    unittest.main()
