"""账户账期跨自然月时仍须失败关闭，不因每月1日清零。"""
import json,sys,tempfile,unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from storage import Publisher,LocalStore,PREFIX,METADATA_RESERVATION,SafetyStop,reserved_class_a

class RollingBudget(unittest.TestCase):
 def setUp(self):
  self.folder=tempfile.TemporaryDirectory();self.store=LocalStore(self.folder.name)
  self.publisher=Publisher(self.store,reserve=0);self.publisher.remaining=METADATA_RESERVATION
 def tearDown(self):self.folder.cleanup()
 def ledger(self):return self.store.read_json(PREFIX+'budgets/rolling-operations.json')
 def test_calendar_month_boundary_does_not_reset_available_budget(self):
  self.publisher.reserve_operations('2026-11-30',500_000);before=self.store.requests['put']
  with self.assertRaises(SafetyStop):self.publisher.reserve_operations('2026-12-01',300_001)
  self.assertEqual(self.store.requests['put'],before)
  self.assertEqual(sum(self.ledger()['days'].values()),500_000)
 def test_reservation_stays_until_the_full_35_date_window_expires(self):
  self.publisher.reserve_operations('2026-11-30',800_000)
  with self.assertRaises(SafetyStop):self.publisher.reserve_operations('2027-01-03',1)
  self.publisher.reserve_operations('2027-01-04',1)
  self.assertEqual(self.ledger()['days'],{'2027-01-04':1})
 def test_batch_crossing_utc_midnight_does_not_expire_budget_early(self):
  # 11月1日晚预留、11月2日凌晨上传；12月3日仍可能处于同一31天账期。
  self.publisher.reserve_operations('2026-11-01',800_000)
  with self.assertRaises(SafetyStop):self.publisher.reserve_operations('2026-12-03',1)
 def test_legacy_monthly_reservations_are_not_silently_discarded(self):
  self.store.put(PREFIX+'budgets/2026-11.json',b'{"reserved_class_a":500000}')
  with self.assertRaises(SafetyStop):self.publisher.reserve_operations('2026-12-01',300_001)
  self.assertIsNone(self.ledger())
  self.publisher.reserve_operations('2026-12-01',10)
  self.assertEqual(self.ledger()['days'],{'2026-12-01':500_010})
 def test_window_can_intersect_three_calendar_months(self):
  self.store.put(PREFIX+'budgets/2026-03.json',b'{"reserved_class_a":500000}')
  self.store.put(PREFIX+'budgets/2026-04.json',b'{"reserved_class_a":300000}')
  with self.assertRaises(SafetyStop):self.publisher.reserve_operations('2026-05-01',1)
 def test_backward_clock_and_corrupt_values_stop(self):
  self.publisher.reserve_operations('2026-12-01',100)
  with self.assertRaises(SafetyStop):self.publisher.reserve_operations('2026-11-30',1)
  for bad in [-1,True,'1']:
   with self.subTest(bad=bad):
    self.store.put(PREFIX+'budgets/rolling-operations.json',json.dumps({'version':1,'window_days':35,'as_of':'2026-12-01','days':{'2026-12-01':bad}}).encode())
    with self.assertRaises(SafetyStop):self.publisher.reserve_operations('2026-12-02',1)
 def test_normal_four_daily_runs_fit_conservative_35_day_budget(self):
  from datetime import date,timedelta
  # 一次准备和四个发布分片，容量按两批以上对象数估算，并含条件写入冲突预算。
  per_run=sum(reserved_class_a(48,times,4000) for times in [9,9,9,9,6])
  for i in range(40):
   for unused in range(4):self.publisher.reserve_operations((date(2026,11,14)+timedelta(days=i)).isoformat(),per_run)
  self.assertEqual(sum(self.ledger()['days'].values()),35*4*per_run)
  self.assertLess(35*4*per_run,800_000)
 def test_two_shards_cannot_reserve_the_same_budget(self):
  day='2026-11-23';self.publisher.reserve_operations(day,790_000)
  barrier=Barrier(2)
  publishers=[Publisher(LocalStore(self.folder.name),reserve=0) for _ in range(2)]
  def reserve(publisher,count):
   original=publisher.store.read_json_version
   def read(key):
    value=original(key)
    if value[0]['days'][day]==790_000:barrier.wait(timeout=5)
    return value
   publisher.store.read_json_version=read
   try:publisher.reserve_operations(day,count)
   except SafetyStop as error:
    self.assertIn('超过80万次',str(error));return 0
   return count
  with patch('storage.time.sleep'),ThreadPoolExecutor(max_workers=2) as threads:
   jobs=[threads.submit(reserve,p,count) for p,count in zip(publishers,[7000,8000])]
   accepted=[job.result() for job in jobs]
  self.assertEqual(accepted.count(0),1)
  self.assertEqual(self.ledger()['days'][day],790_000+sum(accepted))
 def test_corrupt_total_cannot_be_hidden_by_expiring_old_days(self):
  self.store.put(PREFIX+'budgets/rolling-operations.json',json.dumps({'version':1,'window_days':35,'as_of':'2026-12-01','days':{'2026-11-30':500_000,'2026-12-01':400_000}}).encode())
  with self.assertRaises(SafetyStop):self.publisher.reserve_operations('2027-02-01',1)

if __name__=='__main__':unittest.main()
