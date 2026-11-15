import sys
import tempfile
import unittest
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'automation'))
import production as prod
from storage import LocalStore, encoded, PREFIX, Publisher

class AutomaticProduction(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = LocalStore(self.temp.name, 'weather-v1/ifs/full/')
        self.now = datetime(2026,11,15,9,tzinfo=timezone.utc)
        self.run_id = '2026111500-' + 'a'*12
        self.bundle = {'test':'ready'}
        self.prefix = self.store.prefix

    def plan(self, fetch):
        return prod.select_job(self.store, 'IFS', self.now, fetch)

    def test_busy_does_not_touch_sources_or_write(self):
        self.store.put(PREFIX+'writer-lock.json',encoded({'expires':self.now.timestamp()+60}))
        writes=self.store.requests['put']
        self.assertIn('writer active', self.plan(lambda _:self.fail())[0])
        self.assertEqual(self.store.requests['put'], writes)

    def test_ready_wait_and_retry_are_distinct(self):
        seen=[]
        def late(run):
            seen.append(prod.cycle_id(run))
        self.assertIsNone(self.plan(late)[1])
        self.assertEqual(seen,['2026111500','2026111412'])
        self.assertEqual(self.plan(lambda _:self.bundle)[1],self.bundle)
        self.store.put(self.prefix+'scheduler-attempt.json',encoded({'cycle':'2026111500','retry_after':self.now.timestamp()+60}))
        self.assertIsNone(self.plan(lambda _:self.bundle)[1])

    def test_resumes_first_incomplete_even_for_original_18z(self):
        self.store.put(self.prefix+'available.json',encoded({'run_id':'2026111418-'+'b'*12,'complete':False}))
        seen=[]
        self.plan(lambda run:seen.append(prod.cycle_id(run)) or self.bundle)
        self.assertEqual(seen,['2026111418'])

    def test_interrupted_cleanup_repairs_then_same_cycle_is_read_only(self):
        Publisher(self.store)
        times=[{'lead':lead} for lead in prod.LEADS]
        manifest={'run_id':self.run_id,'status':'complete','times':times}
        avail={'run_id':self.run_id,'complete':True,'times':times}
        for key,value in [('runs/'+self.run_id+'/manifest.json',manifest),('available.json',avail),('latest.json',manifest),
                          ('runs/2026111412-bbbbbbbbbbbb/F000/old',{}),('outside-runs.json',{})]:
            self.store.put(self.prefix+key,encoded(value))
        fetch=lambda _:self.fail('已完成起报不应再读源')
        self.assertIsNone(self.plan(fetch)[1])
        self.assertIsNone(self.store.read_json(self.prefix+'runs/2026111412-bbbbbbbbbbbb/F000/old'))
        self.assertEqual(self.store.read_json(self.prefix+'outside-runs.json'),{})
        writes=self.store.requests['put']
        self.assertIsNone(self.plan(fetch)[1])
        self.assertEqual(writes,self.store.requests['put'])
