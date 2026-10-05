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
    def test_current_armed_geometry(self):
        test_rc1_contract_preserves_armed_plan_and_rejects_stale_feed()

if __name__ == "__main__":
    unittest.main()
