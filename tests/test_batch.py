import json,os,sys,tempfile,time,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from storage import LocalStore,Publisher,SafetyStop,PREFIX,OWNER,encoded,run_ids,shard_budget,retained_bytes,CEILING_BYTES,HEADROOM_BYTES,METADATA_RESERVATION,transient_error
from batch import execute,build_plan,acquire_lease,release_lease,valid_time_manifest,download,public_error,stop_detail,error_frames,step_summary,shard_leads,FULL_LEADS,wait_for_prepare,LeaseHeartbeat,LEASE_SECONDS,LEASE_RENEW_SECONDS,execute_stage,StageTimeout,StageFailure,MAX_SKIPS,STAGE_BUDGETS,STAGE_RETRY_FACTOR,TIMESTEP_BUDGET,lease_key,lease_keys,active_lease,active_leases,foreign_leases,LOCK_SHARDS,retry_skipped,transient_storage_error
import batch as batch_module
from unittest.mock import patch,Mock
import unittest.mock as mock
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
    def test_rolling_write_reservation_stops_before_limit(self):
        publisher=Publisher(self.store,reserve=0);publisher.prepare(run_key(1))
        publisher.reserve_operations('2026-11-14',790_000)
        with self.assertRaises(SafetyStop):publisher.reserve_operations('2026-11-14',20_000)
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
        self.assertNotIn(object_key(3),self.store.list())
        self.assertEqual(self.store.read_json(PREFIX+'latest.json'),latest)
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
    def test_ready_time_is_published_before_entire_run(self):
        import hashlib
        publisher=Publisher(self.store,reserve=0);publisher.prepare(run_key(1))
        key=object_key(1,'F003/product.pmtiles');data=b'verified-product';md5=hashlib.md5(data).hexdigest()
        publisher.put(key,data,immutable=True)
        manifest={'lead':3,'valid':'2026-11-13T21:00:00Z','product_bytes':len(data),
            'products':[{'key':'height','status':'verified','object_key':key,'bytes':len(data),'md5':md5}]}
        with self.assertRaises(SafetyStop):publisher.publish_time(manifest)
        publisher.put_json(object_key(1,'F003/manifest.json'),manifest)
        publisher.publish_time(manifest)
        available=self.store.read_json(PREFIX+'available.json')
        self.assertFalse(available['complete']);self.assertEqual([r['lead'] for r in available['times']],[3])
        self.assertIsNone(self.store.read_json(PREFIX+'latest.json'))
        complete={'run_id':run_key(1),'status':'complete'}
        with self.assertRaises(SafetyStop):publisher.finish_run(complete,[3,6])
        publisher.finish_run(complete,[3]);self.assertTrue(self.store.read_json(PREFIX+'available.json')['complete'])

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


class StopReason(unittest.TestCase):
    def test_safety_stop_reason_is_public(self):
        self.assertEqual(public_error(SafetyStop('整批任务接近时间上限'),'r2'),'整批任务接近时间上限')
    def test_unexpected_errors_keep_only_type_and_code(self):
        class Boom(Exception):
            response={'Error':{'Code':'AccessDenied'}}
        self.assertEqual(public_error(Boom('secret AKIAEXAMPLE'),'r2'),'Boom:AccessDenied')
        self.assertEqual(public_error(Boom('<xml>'),'local'),'<xml>')
    def test_plain_exception_keeps_a_short_message(self):
        # 云端日志要能区分「渲染崩了」与「收尾写索引崩了」，所以非 SDK 异常保留消息。
        self.assertEqual(stop_detail(AttributeError("'NoneType' object has no attribute 'get'"),'r2'),
            "AttributeError：'NoneType' object has no attribute 'get'")
        self.assertEqual(stop_detail(StageTimeout('渲染超时：已用600秒，预算600秒'),'r2'),'渲染超时：已用600秒，预算600秒')
    def test_sdk_errors_still_hide_their_message(self):
        from botocore.exceptions import ClientError
        error=ClientError({'Error':{'Code':'AccessDenied','Message':'AKIAEXAMPLE'}},'PutObject')
        detail=stop_detail(error,'r2');self.assertEqual(detail,'ClientError:AccessDenied');self.assertNotIn('AKIA',detail)
    def test_frames_point_inside_the_repository_only(self):
        try:
            raise AttributeError('boom')
        except AttributeError as error:
            rows=error_frames(error)
        self.assertTrue(rows and rows[-1].startswith('tests/test_batch.py:'))
        self.assertFalse(any('/tmp' in row or 'site-packages' in row for row in rows))
    def test_summary_is_written_when_github_provides_a_path(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'summary.md'
            with patch.dict(os.environ,{'GITHUB_STEP_SUMMARY':str(path)}):
                step_summary(['### 制图停止','- 原因：测试原因'])
            self.assertIn('原因：测试原因',path.read_text())
    def test_summary_is_silently_skipped_without_env(self):
        with patch.dict(os.environ,{},clear=True):step_summary(['x'])

class ShardBudget(unittest.TestCase):
    def test_all_shards_together_stay_below_the_ceiling(self):
        for shards in (1,2,4,8):
            budget=shard_budget(shards)
            total=budget*shards+HEADROOM_BYTES+METADATA_RESERVATION*(shards+2)
            self.assertLessEqual(total,CEILING_BYTES)
    def test_retained_batch_reduces_the_shard_allowance(self):
        self.assertLess(shard_budget(4,2_000_000_000),shard_budget(4,0))
        # 回归保护：4 片时每片额度必须装得下 9 个实测时次（126,663,612 B/时次）。
        self.assertGreaterEqual(shard_budget(4,126_663_612*33),126_663_612*9)
        self.assertGreaterEqual(shard_budget(4,0),126_663_612*9)
    def test_no_room_left_is_refused(self):
        with self.assertRaises(SafetyStop):shard_budget(1,CEILING_BYTES)
        with self.assertRaises(SafetyStop):shard_budget(0)
    def test_writer_cannot_exceed_its_shard_budget(self):
        with tempfile.TemporaryDirectory() as directory:
            store=LocalStore(Path(directory)/'store')
            publisher=Publisher(store,reserve=0,budget=1_000)
            publisher.prepare(run_key(1));before=store.requests['put'];base=publisher.written
            with self.assertRaises(SafetyStop):publisher.put(object_key(1,'data'),b'x'*2_000)
            self.assertEqual(store.requests['put'],before)
            publisher.put(object_key(1,'small'),b'x'*400)
            self.assertEqual(publisher.written-base,400)
    def test_writer_without_budget_is_unlimited(self):
        with tempfile.TemporaryDirectory() as directory:
            store=LocalStore(Path(directory)/'store')
            publisher=Publisher(store,reserve=0);publisher.prepare(run_key(1));base=publisher.written
            publisher.put(object_key(1,'data'),b'x'*2_000)
            self.assertEqual(publisher.written-base,2_000)


class Shards(unittest.TestCase):
    def test_four_shards_cover_the_full_table_exactly_once(self):
        parts=[shard_leads(i,4) for i in range(4)]
        self.assertEqual([len(p) for p in parts],[9,9,9,6])
        self.assertEqual([lead for part in parts for lead in part],FULL_LEADS)
        self.assertEqual(parts[0][0],0);self.assertEqual(parts[-1][-1],96)
    def test_even_smaller_shards_still_cover_everything(self):
        for count in (1,2,3,4,8,16):
            parts=[shard_leads(i,count) for i in range(count)]
            self.assertEqual([lead for part in parts for lead in part],FULL_LEADS)
    def test_invalid_shard_requests_are_refused(self):
        for args in ((0,0),(4,4),(-1,4),(0,17),(16,16)):
            with self.assertRaises(SafetyStop):shard_leads(*args)


class ConcurrentShards(unittest.TestCase):
    def _publish(self,store,lead):
        import hashlib
        publisher=Publisher(store,reserve=0);publisher.target=run_key(1)
        key=object_key(1,f'F{lead:03}/product.pmtiles');data=b'p%d'%lead;md5=hashlib.md5(data).hexdigest()
        publisher.put(key,data,immutable=True)
        manifest={'lead':lead,'valid':'2026-11-13T21:00:00Z','product_bytes':len(data),
            'products':[{'key':'height','status':'verified','object_key':key,'bytes':len(data),'md5':md5}]}
        publisher.put_json(object_key(1,f'F{lead:03}/manifest.json'),manifest)
        publisher.publish_time(manifest)
    def test_two_shards_keep_both_time_lists(self):
        with tempfile.TemporaryDirectory() as directory:
            store=LocalStore(Path(directory)/'store')
            head=Publisher(store,reserve=0);head.prepare(run_key(1))
            # 两个 Publisher 实例模拟两个批次进程各写各的时次。
            self._publish(store,3);self._publish(store,6)
            available=store.read_json(PREFIX+'available.json')
            self.assertEqual([row['lead'] for row in available['times']],[3,6])
    def test_shard_budget_is_split_across_the_two_shards(self):
        retained=4_180_000_000
        self.assertLessEqual(shard_budget(2,retained)*2+retained+HEADROOM_BYTES,CEILING_BYTES)
        # 单片需要约 2.1 GB，两批并存时仍必须放得下。
        self.assertGreater(shard_budget(2,retained),2_100_000_000)


class LeaseHeartbeatTest(unittest.TestCase):
    def _store(self,directory):
        return LocalStore(Path(directory)/'store')
    def test_heartbeat_renews_expiry(self):
        with tempfile.TemporaryDirectory() as directory:
            store=self._store(directory)
            store.put(PREFIX+'writer-lock.json',encoded({'token':'a','expires':time.time()+1}))
            beat=LeaseHeartbeat(store,'a',seconds=600)
            self.assertTrue(beat.beat());self.assertEqual(beat.beats,1);self.assertFalse(beat.lost)
            self.assertGreater(store.read_json(PREFIX+'writer-lock.json')['expires'],time.time()+500)
    def test_cancelled_process_lease_expires_within_ttl(self):
        # 取消（SIGTERM）后心跳停止，锁只能在 LEASE_SECONDS 内自然过期；
        # 常量必须是分钟级，不能让「空锁 7 小时」的回归悄悄回来。
        self.assertLessEqual(LEASE_SECONDS,1800)
        self.assertLess(LEASE_RENEW_SECONDS,LEASE_SECONDS)
        with tempfile.TemporaryDirectory() as directory:
            store=self._store(directory)
            store.put(PREFIX+'writer-lock.json',encoded({'token':'dead','expires':time.time()-1}))
            self.assertLess(store.read_json(PREFIX+'writer-lock.json')['expires'],time.time())
class ShardHandshake(unittest.TestCase):
    def test_second_shard_stops_when_prepare_never_happens(self):
        store=Mock();store.read_json=Mock(return_value=None)
        with self.assertRaises(SafetyStop):wait_for_prepare(store,PREFIX,run_key(1),timeout=0.01,interval=0)
    def test_second_shard_proceeds_once_marker_exists(self):
        store=Mock();store.read_json=Mock(return_value={'status':'incomplete'})
        self.assertEqual(wait_for_prepare(store,PREFIX,run_key(1),timeout=0.01,interval=0),PREFIX+f'runs/{run_key(1)}/progress-p0.json')
    def test_old_batch_bytes_are_not_counted_as_new(self):
        with tempfile.TemporaryDirectory() as directory:
            store=LocalStore(Path(directory)/'store');head=Publisher(store,reserve=0);head.prepare(run_key(1))
            head.put(object_key(1,'blob'),b'x'*100)
            self.assertEqual(retained_bytes(head,run_key(1)),0)
            self.assertEqual(retained_bytes(head,run_key(2)),100)
            self.assertLess(shard_budget(2,retained_bytes(head,run_key(2))),shard_budget(2,0))

class StageFailures(unittest.TestCase):
    """非零退出与超时一样：重试一次，仍失败只跳过这个时次，并把「哪一阶段、退出码几」写进公开日志。"""
    def setUp(self):
        self.monitor=Mock();self.resets=[]
    def test_failure_message_is_public_and_specific(self):
        directory=tempfile.TemporaryDirectory();self.addCleanup(directory.cleanup)
        log=Path(directory.name)/'stage.log'
        with mock.patch('batch.subprocess.Popen') as popen,mock.patch('batch.os.killpg'):
            popen.return_value.poll.return_value=7;popen.return_value.returncode=7;log.write_text('x')
            with self.assertRaises(StageFailure) as caught:execute(['node','x.mjs'],self.monitor,log,label='渲染')
        self.assertIsInstance(caught.exception,SafetyStop)
        self.assertIn('渲染',str(caught.exception));self.assertIn('7',str(caught.exception))
    def test_failure_retries_once_then_propagates(self):
        calls=[]
        def boom(*_args,**_kwargs):
            calls.append(1);raise StageFailure('渲染退出码 1')
        with mock.patch('batch.execute',side_effect=boom):
            with self.assertRaises(StageFailure):execute_stage(['node','x.mjs'],self.monitor,Mock(),300,'渲染',reset=lambda:self.resets.append(1))
        self.assertEqual(len(calls),2);self.assertEqual(self.resets,[1])
    def test_skip_budget_is_small(self):
        # 少量时次失败可以跳过；连续失败说明是系统性故障，必须整体停下。
        self.assertLessEqual(MAX_SKIPS,2)

class ForeignLeases(unittest.TestCase):
    """清旧批之前要能认出「别人还在写」，否则多清一次就毁掉别人正在发布的成果。"""
    def _store(self):
        directory=tempfile.TemporaryDirectory();self.addCleanup(directory.cleanup)
        return LocalStore(Path(directory.name)/'store')
    def test_own_lease_is_not_foreign(self):
        store=self._store()
        store.put(PREFIX+'writer-lock-p0.json',encoded({'token':'mine','expires':time.time()+60}))
        self.assertEqual(foreign_leases(store,'mine'),[])
    def test_another_live_lease_is_foreign(self):
        store=self._store()
        store.put(PREFIX+'writer-lock-p1.json',encoded({'token':'other','expires':time.time()+60}))
        self.assertEqual([lock['token'] for lock in foreign_leases(store,'mine')],['other'])
    def test_expired_foreign_lease_is_ignored(self):
        store=self._store()
        store.put(PREFIX+'writer-lock.json',encoded({'token':'other','expires':time.time()-1}))
        self.assertEqual(foreign_leases(store,'mine'),[])
    def test_shard_wait_is_minutes_not_quarters(self):
        # 准备已由单独 job 完成，等不到标记就说明出事了，不能再空等一刻钟。
        import inspect
        default=inspect.signature(wait_for_prepare).parameters['timeout'].default
        self.assertLessEqual(default,300)

class StageBudgets(unittest.TestCase):
    """单时次阶段超时：先按 1.5 倍预算重试一次，仍超时只跳过这个时次。"""
    def setUp(self):
        self.budgets=[];self.resets=[];self.monitor=Mock()
    def fake(self,command,monitor,log,timeout=1200,label='处理阶段'):
        self.budgets.append(timeout)
        if len(self.budgets)==1:raise StageTimeout(f'{label}超时')
        return 1.5
    def test_budgets_stay_inside_the_batch_fuse(self):
        # 每片 9 个时次、单时次 25 分钟 → 最坏 3.75 小时，仍低于 5 小时熔断；重试不会突破单时次预算。
        self.assertEqual(STAGE_BUDGETS,{'extract':600,'regrid':600,'render':1200})
        self.assertGreater(STAGE_RETRY_FACTOR,1)
        self.assertEqual(int(STAGE_BUDGETS['render']*STAGE_RETRY_FACTOR),1800)
        self.assertLess(TIMESTEP_BUDGET*9,5*3600)
    def test_retry_is_capped_by_the_remaining_time_budget(self):
        deadline=time.monotonic()+0.0
        def boom(command,monitor,log,timeout=1200,label='处理阶段'):
            self.budgets.append(timeout);raise StageTimeout('超时')
        with patch.object(batch_module,'execute',boom):
            with self.assertRaises(StageTimeout):execute_stage(['x'],self.monitor,Mock(),600,'抽取',deadline=deadline)
        self.assertEqual(self.budgets,[])
    def test_timeout_retries_once_with_wider_budget(self):
        with patch.object(batch_module,'execute',self.fake):
            seconds=execute_stage(['x'],self.monitor,Mock(),600,'抽取',reset=lambda:self.resets.append('抽取'))
        self.assertEqual(self.budgets,[600,900])
        self.assertEqual(self.resets,['抽取'])
        self.assertEqual(seconds,1.5)
    def test_second_timeout_is_raised_to_skip_the_time(self):
        def always(command,monitor,log,timeout=1200,label='处理阶段'):
            self.budgets.append(timeout);raise StageTimeout('超时')
        with patch.object(batch_module,'execute',always):
            with self.assertRaises(StageTimeout):execute_stage(['x'],self.monitor,Mock(),600,'抽取')
        self.assertEqual(self.budgets,[600,900])
    def test_non_timeout_error_is_not_retried(self):
        def boom(command,monitor,log,timeout=1200,label='处理阶段'):
            self.budgets.append(timeout);raise RuntimeError('处理阶段退出码 1')
        with patch.object(batch_module,'execute',boom):
            with self.assertRaises(RuntimeError):execute_stage(['x'],self.monitor,Mock(),1200,'渲染')
        self.assertEqual(self.budgets,[1200])
    def test_stage_timeout_stays_a_safety_stop(self):
        self.assertTrue(issubclass(StageTimeout,SafetyStop))
        self.assertIn('超时',public_error(StageTimeout('渲染超时：已用1801秒，预算1800秒'),'r2'))

class ShardLeases(unittest.TestCase):
    """四片并行：每片一把写入租约，同片仍然互斥；判定「有人在写」要看全部租约。"""
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name);self.store=LocalStore(self.root/'store')
    def tearDown(self):self.temp.cleanup()
    def test_each_shard_holds_its_own_lease(self):
        first=acquire_lease(self.store,(0,4));second=acquire_lease(self.store,(1,4))
        self.addCleanup(first.close);self.addCleanup(second.close)
        with self.assertRaises(SafetyStop):acquire_lease(self.store,(0,4))
    def test_unsharded_lease_still_uses_the_base_key(self):
        self.assertEqual(lease_key(),PREFIX+'writer-lock.json')
        self.assertEqual(lease_key((2,4)),PREFIX+'writer-lock-p2.json')
    def test_any_live_shard_lock_counts_as_active(self):
        self.assertIsNone(active_lease(self.store))
        self.store.put(PREFIX+'writer-lock-p2.json',encoded({'token':'t','expires':time.time()+60}))
        key,lock=active_lease(self.store)
        self.assertEqual(key,PREFIX+'writer-lock-p2.json');self.assertEqual(lock['token'],'t')
    def test_expired_shard_lock_is_not_active(self):
        self.store.put(PREFIX+'writer-lock-p3.json',encoded({'token':'t','expires':time.time()-1}))
        self.assertIsNone(active_lease(self.store))
    def test_lease_keys_cover_base_and_every_shard(self):
        keys=lease_keys()
        self.assertEqual(len(keys),LOCK_SHARDS+1)
        self.assertEqual(len(set(keys)),len(keys))

class SkippedTimestepRetry(unittest.TestCase):
    """跳过的时次在整批末尾再试一次：网络抖动、内存压力通常那时已恢复。"""
    def setUp(self):
        self.plan=[{'lead':0},{'lead':3},{'lead':6}]
        self.logs=[]
    def log(self,*args,**kwargs):self.logs.append(args[0])
    def test_recovered_timestep_leaves_the_failure_list(self):
        failures=[{'lead':3,'reason':'渲染（第2次）退出码 1'}]
        recovered=retry_skipped(failures,self.plan,lambda item:None,time.monotonic(),log=self.log)
        self.assertEqual(recovered,[3]);self.assertEqual(failures,[])
        self.assertIn('"status": "retry"',self.logs[0]);self.assertIn('"status": "recovered"',self.logs[1])
    def test_still_failing_timestep_keeps_the_latest_reason(self):
        failures=[{'lead':6,'reason':'抽取（第2次）超时'}]
        recovered=retry_skipped(failures,self.plan,lambda item:'渲染（第2次）退出码 2',time.monotonic(),log=self.log)
        self.assertEqual(recovered,[]);self.assertEqual(failures,[{'lead':6,'reason':'渲染（第2次）退出码 2'}])
    def test_no_retry_when_the_batch_budget_is_nearly_spent(self):
        failures=[{'lead':0,'reason':'x'}];calls=[]
        recovered=retry_skipped(failures,self.plan,lambda item:calls.append(item),time.monotonic()-(5*3600-60),log=self.log)
        self.assertEqual(calls,[]);self.assertEqual(recovered,[]);self.assertEqual(len(failures),1)
    def test_no_retry_when_the_write_lease_is_gone(self):
        failures=[{'lead':0,'reason':'x'}];calls=[]
        recovered=retry_skipped(failures,self.plan,lambda item:calls.append(item),time.monotonic(),keep_going=lambda:False,log=self.log)
        self.assertEqual(calls,[]);self.assertEqual(recovered,[])
    def test_retry_budget_stays_inside_the_batch_fuse(self):
        import inspect
        reserve=inspect.signature(retry_skipped).parameters['reserve'].default
        self.assertGreaterEqual(reserve,300)
        self.assertLess(reserve,5*3600)

class TransientStorage(unittest.TestCase):
    """R2 偶发 5xx 不该把整片判红：按内容寻址重试即可补齐。"""
    def client_error(self,code,status):
        from botocore.exceptions import ClientError
        return ClientError({'Error':{'Code':code},'ResponseMetadata':{'HTTPStatusCode':status}},'PutObject')
    def test_server_side_errors_are_transient(self):
        for code,status in (('InternalError',500),('ServiceUnavailable',503),('SlowDown',429)):
            self.assertTrue(transient_storage_error(self.client_error(code,status)),code)
    def test_other_failures_are_not_transient(self):
        self.assertFalse(transient_storage_error(self.client_error('AccessDenied',403)))
        self.assertFalse(transient_storage_error(SafetyStop('写入租约归属或有效期异常')))
        self.assertFalse(transient_storage_error(ValueError('boom')))
    def test_heartbeat_keeps_running_through_transient_failures(self):
        store=FlakyStore(self.client_error('InternalError',500),failures=1)
        beat=LeaseHeartbeat(store,'t',key=PREFIX+'writer-lock-p0.json')
        self.assertTrue(beat.beat());self.assertEqual(beat.errors,1);self.assertFalse(beat.lost)
        self.assertTrue(beat.beat());self.assertEqual(beat.errors,0);self.assertEqual(beat.beats,1)
    def test_heartbeat_gives_up_after_three_failed_beats(self):
        store=FlakyStore(self.client_error('InternalError',500),failures=3)
        beat=LeaseHeartbeat(store,'t',key=PREFIX+'writer-lock-p0.json')
        self.assertTrue(beat.beat());self.assertTrue(beat.beat())
        self.assertFalse(beat.beat());self.assertTrue(beat.lost)
    def test_release_retries_transient_delete_failures(self):
        store=FlakyStore(self.client_error('InternalError',500),failures=2,on='delete')
        with mock.patch('batch.time.sleep'):
            release_lease(store,'t',(0,4))
        self.assertEqual(store.deletes,1)
    def test_release_gives_up_after_attempts(self):
        store=FlakyStore(self.client_error('InternalError',500),failures=9,on='delete')
        with mock.patch('batch.time.sleep'):
            with self.assertRaises(Exception):release_lease(store,'t',(0,4))
        self.assertEqual(store.deletes,0)

class FlakyStore:
    """按次数抛临时故障的假存储：read_json/put 失败与 delete 失败分开统计。"""
    def __init__(self,error,failures=1,on='put'):
        self.error=error;self.remaining=failures;self.on=on;self.deletes=0;self.token='t'
        self.requests={'get':0,'put':0,'delete':0}
    def read_json(self,key):
        self.requests['get']+=1
        return {'token':self.token,'expires':time.time()+600}
    def put(self,key,payload):
        self.requests['put']+=1
        if self.on=='put' and self.remaining>0:
            self.remaining-=1;raise self.error
    def delete(self,keys):
        self.requests['delete']+=1
        if self.on=='delete' and self.remaining>0:
            self.remaining-=1;raise self.error
        self.deletes+=1

class ErrorReporting(unittest.TestCase):
    """异常本身也要能被安全格式化：连接超时没有 response，不能反过来把真实原因顶掉。"""
    def timeout_error(self):
        from botocore.exceptions import ConnectionClosedError
        return ConnectionClosedError(endpoint_url='https://example.r2.cloudflarestorage.com')
    def test_public_error_survives_response_less_errors(self):
        error=self.timeout_error()
        self.assertEqual(public_error(error,'r2'),'ConnectionClosedError')
        self.assertIn('ConnectionClosedError',stop_detail(error,'r2'))
    def test_timeouts_count_as_transient(self):
        self.assertTrue(transient_storage_error(self.timeout_error()))
        self.assertTrue(transient_error(self.timeout_error()))

class Fingerprint(unittest.TestCase):
    """run_id 由源指纹派生，客户端缓存键含 run_id：基础设施代码的改动不该作废用户缓存。"""
    def test_only_tile_sources_change_fingerprint(self):
        with tempfile.TemporaryDirectory() as name:
            root=Path(name)
            for folder in ('src','scripts'):(root/folder).mkdir()
            (root/'src'/'tiles.mjs').write_text('export const a=1;\n')
            (root/'scripts'/'clean_tiles.py').write_text('print(1)\n')
            (root/'scripts'/'batch.py').write_text('print(2)\n')
            orig_root,orig_tile=batch_module.ROOT,batch_module.TILE_SOURCES
            try:
                batch_module.ROOT=root
                base=batch_module.source_fingerprint()
                (root/'scripts'/'batch.py').write_text('print(3)\n')
                self.assertEqual(base,batch_module.source_fingerprint())
                (root/'src'/'tiles.mjs').write_text('export const a=2;\n')
                self.assertNotEqual(base,batch_module.source_fingerprint())
            finally:
                batch_module.ROOT, batch_module.TILE_SOURCES = orig_root,orig_tile

if __name__=='__main__':unittest.main()

