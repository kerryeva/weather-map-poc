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
        self.assertEqual(seen,['2026111506','2026111500','2026111418','2026111412'])
        self.assertEqual(self.plan(lambda _:self.bundle)[1],self.bundle)
        self.store.put(self.prefix+'scheduler-attempt.json',encoded({'cycle':'2026111506','retry_after':self.now.timestamp()+60}))
        self.assertIsNone(self.plan(lambda _:self.bundle)[1])

    def test_larger_queue_retries_interrupted_cycle_once(self):
        attempt = {'cycle':'2026111500', 'retry_after':self.now.timestamp()+3600}
        key = self.prefix+'scheduler-attempt.json'
        self.store.put(self.prefix+'available.json', encoded({'run_id':self.run_id,'complete':False}))
        self.store.put(key, encoded(attempt))
        def plan(limit):
            return prod.select_job(self.store, 'IFS', self.now, lambda _:self.bundle, limit)[1]
        self.assertIsNone(plan(1_000_000))
        self.assertEqual(plan(2_000_000), self.bundle)
        self.store.put(key, encoded({**attempt, 'queue_limit':2_000_000}))
        self.assertIsNone(plan(2_000_000))

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
        def fetch(run):
            self.assertEqual(prod.cycle_id(run),'2026111506')
            return None
        self.assertIsNone(self.plan(fetch)[1])
        self.assertIsNone(self.store.read_json(self.prefix+'runs/2026111412-bbbbbbbbbbbb/F000/old'))
        self.assertEqual(self.store.read_json(self.prefix+'outside-runs.json'),{})
        writes=self.store.requests['put']
        self.assertIsNone(self.plan(fetch)[1])
        self.assertEqual(writes,self.store.requests['put'])

    def test_resume_keeps_processor_while_new_cycle_uses_current(self):
        current = 'c' * 40
        old = 'b' * 40
        self.store.put(self.prefix+'available.json', encoded({'run_id': self.run_id, 'complete':False}))
        self.store.put(self.prefix+'runs/'+self.run_id+'/progress.json', encoded({'processor_commit': old}))
        self.assertEqual(prod.processor_revision(self.store,'2026111500',current),old)
        self.assertEqual(prod.processor_revision(self.store,'2026111512',current),current)

    def test_legacy_first_run_recovers_with_original_processor(self):
        self.store.put(self.prefix+'available.json', encoded({'run_id':'2026111418-b72254d378ed','complete':False}))
        self.assertEqual(prod.processor_revision(self.store,'2026111418','a'*40),
                         'b6340d697af566e3ffaf96f595c729c88bb698c7')

    def test_four_cycles_in_order_across_midnight(self):
        now = datetime(2026, 11, 16, 0, tzinfo=timezone.utc)
        runs = prod.candidate_runs(now, '2026111418', None)
        self.assertEqual([prod.cycle_id(run) for run in runs],
                         ['2026111500','2026111506','2026111512','2026111518','2026111600'])

    def test_oldest_ready_cycle_and_unfinished_cycle_take_priority(self):
        self.store.put(self.prefix+'latest.json', encoded({'run_id':'2026111418-'+'a'*12}))
        self.assertIn('2026111500', self.plan(lambda _:self.bundle)[0])
        self.assertIn('2026111506', self.plan(lambda run:self.bundle if run.hour==6 else None)[0])
        self.store.put(self.prefix+'available.json', encoded({'run_id':self.run_id,'complete':False}))
        self.assertIn('2026111500', self.plan(lambda _:self.bundle)[0])


    def test_four_day_upgrade_removes_obsolete_objects_once(self):
        Publisher(self.store)
        old = self.prefix+'runs/'+self.run_id+'/F000/old.pmtiles'
        self.store.put(old, b'old')
        self.store.put(self.prefix+'available.json', encoded({'run_id':self.run_id}))
        self.store.put(self.prefix+'scheduler-attempt.json', encoded({'retry_after':9999999999}))
        self.store.put(PREFIX+'budgets/rolling-operations.json', encoded({'preserve':True}))
        prod.upgrade_four_day_policy(self.store)
        self.assertIsNone(self.store.read_json(self.prefix+'available.json'))
        self.assertNotIn(old,self.store.list())
        self.assertEqual(self.store.read_json(PREFIX+'budgets/rolling-operations.json'),{'preserve':True})
        self.store.put(old,b'new')
        prod.upgrade_four_day_policy(self.store)
        self.assertIn(old,self.store.list())

    def test_four_day_upgrade_waits_for_active_writer(self):
        self.store.put(PREFIX+'writer-lock.json',encoded({'expires':datetime.now(timezone.utc).timestamp()+60}))
        before=self.store.requests['put']
        prod.upgrade_four_day_policy(self.store)
        self.assertEqual(before,self.store.requests['put'])

    def test_four_day_upgrade_rejects_unknown_owned_prefix(self):
        self.store.put(self.prefix+'owner.json',encoded({'protocol':'someone-else'}))
        with self.assertRaises(prod.SafetyStop):
            prod.upgrade_four_day_policy(self.store)

    def test_forecast_policy_has_33_times_and_finishes_at_96(self):
        self.assertEqual(len(prod.LEADS),33)
        self.assertEqual(prod.LEADS[-1],96)
