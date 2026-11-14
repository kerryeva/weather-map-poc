import json,sys,tempfile,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from storage import LocalStore,Publisher,SafetyStop,PREFIX,OWNER,encoded,run_ids
from batch import build_plan,acquire_lease,release_lease,valid_time_manifest,download
from unittest.mock import patch,Mock
import requests

def run_key(i):return f'{i:010}-'+'a'*12
def object_key(i,name='manifest.json'):return PREFIX+'runs/'+run_key(i)+'/'+name

class Fuses(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name);self.store=LocalStore(self.root/'store')
    def tearDown(self):self.temp.cleanup()
    def seed(self,key,data=b'x'):
        # 模拟其他生产者留下的对象，不能借测试存储接口绕过前缀限制。
        path=self.store.root/key;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(data)
    def owner(self):self.seed(PREFIX+'owner.json',encoded(OWNER))
    def test_three_batches_stop_without_deleting_anything(self):
        self.owner()
        for i in (1,2,3):self.seed(object_key(i))
        with self.assertRaises(SafetyStop):Publisher(self.store,reserve=0)
        self.assertEqual(self.store.requests['delete'],0)
        self.assertEqual(len(run_ids(self.store.list())),3)
    def test_unknown_ownership_is_not_adopted_or_removed(self):
        self.seed(object_key(1))
        with self.assertRaises(SafetyStop):Publisher(self.store,reserve=0)
        self.assertEqual(self.store.requests['delete'],0)
    def test_keep_latest_and_delete_only_older_managed_batch(self):
        self.owner();self.seed(object_key(1));self.seed(object_key(2))
        self.seed('unmanaged-sample',b'leave me')
        latest={'run_id':run_key(2),'status':'complete'}
        self.seed(PREFIX+'latest.json',encoded(latest));self.seed(object_key(2),encoded(latest))
        publisher=Publisher(self.store,reserve=0);publisher.prepare(run_key(3))
        objects=self.store.list();self.assertNotIn(object_key(1),objects)
        self.assertIn(object_key(2),objects);self.assertIn('unmanaged-sample',objects)
    def test_already_over_ceiling_stops_before_any_upload(self):
        self.owner();self.seed('outside-scope',b'x'*1000);before=self.store.requests['put']
        with self.assertRaises(SafetyStop):Publisher(self.store,ceiling=900,reserve=0)
        self.assertEqual(self.store.requests['put'],before)
    def test_risky_next_time_is_rejected_before_put(self):
        publisher=Publisher(self.store,ceiling=3_000_000,reserve=256_000)
        publisher.prepare(run_key(1));path=self.root/'large.bin';path.write_bytes(b'x'*1_000_000)
        before=self.store.requests['put']
        with self.assertRaises(SafetyStop):publisher.reserve_time([(object_key(1,'data'),path)])
        self.assertEqual(self.store.requests['put'],before)
    def test_files_use_one_remote_snapshot_for_entire_time(self):
        publisher=Publisher(self.store,reserve=0);publisher.prepare(run_key(1))
        path=self.root/'small';path.write_bytes(b'1234');files=[(object_key(1,f'p{i}'),path) for i in range(54)]
        before=self.store.requests['list'];publisher.reserve_time(files)
        for key,source in files:publisher.put(key,source,immutable=True)
        self.assertEqual(self.store.requests['list']-before,1)
        self.assertEqual(len(run_ids(publisher.objects)),1)
    def test_reservation_cannot_be_exceeded(self):
        publisher=Publisher(self.store,reserve=0);publisher.prepare(run_key(1));publisher.remaining=2
        before=self.store.requests['put']
        with self.assertRaises(SafetyStop):publisher.put(object_key(1,'data'),b'123')
        self.assertEqual(self.store.requests['put'],before)
    def test_storage_errors_fail_closed(self):
        publisher=Publisher(self.store,reserve=0);publisher.prepare(run_key(1))
        def fail():raise ConnectionError('simulated listing failure')
        self.store.list=fail;before=self.store.requests['put']
        with self.assertRaises(ConnectionError):publisher.reserve_time([])
        self.assertEqual(self.store.requests['put'],before)
    def test_monthly_write_reservation_stops_before_limit(self):
        publisher=Publisher(self.store,reserve=0);publisher.prepare(run_key(1))
        publisher.reserve_month('2026-11',790_000)
        with self.assertRaises(SafetyStop):publisher.reserve_month('2026-11',20_000)
    def test_completed_latest_is_not_changed_by_failed_upload(self):
        self.owner();latest={'run_id':run_key(1),'status':'complete'}
        self.seed(PREFIX+'latest.json',encoded(latest));self.seed(object_key(1),encoded(latest))
        publisher=Publisher(self.store,reserve=0);publisher.prepare(run_key(2));publisher.remaining=1
        with self.assertRaises(SafetyStop):publisher.put(object_key(2,'data'),b'123')
        self.assertEqual(self.store.read_json(PREFIX+'latest.json'),latest)
    def test_cleanup_failure_stops_before_new_batch(self):
        self.owner();latest={'run_id':run_key(2),'status':'complete'}
        self.seed(object_key(1));self.seed(object_key(2),encoded(latest));self.seed(PREFIX+'latest.json',encoded(latest))
        publisher=Publisher(self.store,reserve=0)
        def fail(keys):raise ConnectionError('simulated cleanup failure')
        self.store.delete=fail;before=self.store.requests['put']
        with self.assertRaises(ConnectionError):publisher.prepare(run_key(3))
        self.assertEqual(self.store.requests['put'],before)
    def test_missing_latest_does_not_delete_completed_batch(self):
        self.owner();self.seed(object_key(1),encoded({'status':'complete'}))
        publisher=Publisher(self.store,reserve=0)
        with self.assertRaises(SafetyStop):publisher.prepare(run_key(2))
        self.assertEqual(self.store.requests['delete'],0)
    def test_exclusive_writer_lock(self):
        one=acquire_lease(self.store)
        try:
            with self.assertRaises(SafetyStop):acquire_lease(self.store)
        finally:release_lease(self.store,one)

class Plan(unittest.TestCase):
    def test_empty_or_incomplete_resume_manifest_cannot_skip_processing(self):
        definition={'products':[{'key':'IFS-500-height-default'}]}
        with self.assertRaises(SafetyStop):valid_time_manifest({'processing_sha256':'a','products':[]},definition,3,'a')
        bad={'processing_sha256':'a','products':[{'key':'IFS-500-height-default','status':'not_applicable'}]}
        with self.assertRaises(SafetyStop):valid_time_manifest(bad,definition,3,'a')
    def test_41_times_and_immutable_run_urls(self):
        from datetime import datetime,timedelta,timezone
        run=datetime(2026,11,13,18,tzinfo=timezone.utc)
        meta={'completed':True,'reference_time':run.isoformat(),'valid_times':[(run+timedelta(hours=i)).isoformat() for i in range(0,121,3)]}
        _,plan=build_plan(meta,list(range(0,121,3)))
        self.assertEqual(len(plan),41);self.assertEqual(plan[-1]['lead'],120)
        self.assertIn('/2026/11/13/1800Z/2026-11-18T1800.om',plan[-1]['url'])
        for bad in ([0,0],[1],[123],[]):
            with self.assertRaises(ValueError):build_plan(meta,bad)
        meta['completed']=False
        with self.assertRaises(ValueError):build_plan(meta,[0])

class SourceRetries(unittest.TestCase):
    def test_network_retry_is_bounded_and_recorded(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'source.om';monitor=Mock()
            with patch('batch._download_once',side_effect=[requests.ConnectionError(),{'bytes':20}]) as fetch,patch('batch.time.sleep'):
                result=download('https://example.test',path,monitor,Mock())
            self.assertEqual(result['attempts'],2);self.assertEqual(fetch.call_count,2)
    def test_safety_stop_is_never_retried(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch('batch._download_once',side_effect=SafetyStop('disk guard')) as fetch:
                with self.assertRaises(SafetyStop):download('https://example.test',Path(directory)/'source.om',Mock(),Mock())
            self.assertEqual(fetch.call_count,1)

if __name__=='__main__':unittest.main()
