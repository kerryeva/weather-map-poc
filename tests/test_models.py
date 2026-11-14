"""双模式输入、矩阵及不发布边界的确定性回归。"""
import copy,json,os,subprocess,sys,tempfile,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from freeze_models import validate_bundle
from measure_models import plan_sources
ROOT=Path(__file__).resolve().parents[1]

class DualModels(unittest.TestCase):
 def setUp(self):self.bundle=json.loads((ROOT/'benchmarks/dual-20261114T00Z.json').read_text())

 def test_complete_same_run(self):
  self.assertEqual(validate_bundle(self.bundle).isoformat(),'2026-11-14T00:00:00+00:00')
  for model in ('IFS','GFS'):
   _,plan,spec=plan_sources(self.bundle,model,'full',list(range(0,121,3)))
   self.assertEqual(len(plan),41);self.assertEqual(len(spec['products']),54);self.assertEqual(len(spec['fields']),43)
   self.assertEqual(len(plan[1]['sources']),1 if model=='IFS' else 2)
   self.assertIn('/2026/11/14/0000Z/2026-11-14T0300.om',plan[1]['sources'][0]['url'])

 def test_mixed_run_rejected(self):
  self.bundle['ncep_gfs013']=copy.deepcopy(self.bundle['ecmwf_ifs025'])
  self.bundle['ncep_gfs013']['reference_time']='2026-11-13T18:00:00Z'
  with self.assertRaises(ValueError):validate_bundle(self.bundle)

 def test_missing_domain_or_lead_rejected(self):
  self.bundle.pop('ncep_gfs013')
  with self.assertRaises(ValueError):validate_bundle(self.bundle)
  self.setUp();self.bundle['ncep_gfs025']['valid_times']=[]
  with self.assertRaises(ValueError):validate_bundle(self.bundle)

 def test_missing_variable_stops_instead_of_dropping_product(self):
  self.bundle['ncep_gfs013']['variables'].remove('relative_humidity_2m')
  with self.assertRaises(ValueError):plan_sources(self.bundle,'GFS','full',[3])

 def test_matrix_interleaves_both_models(self):
  with tempfile.TemporaryDirectory() as folder:
   output=Path(folder)/'output'
   subprocess.run([sys.executable,str(ROOT/'scripts/freeze_models.py'),'--metadata',str(ROOT/'benchmarks/dual-20261114T00Z.json')],
      env={**os.environ,'GITHUB_OUTPUT':str(output)},capture_output=True,text=True,check=True,timeout=15)
   matrix=json.loads(output.read_text().split('=',1)[1])['include']
   self.assertEqual(len(matrix),82);self.assertEqual(matrix[:2],[{'model':'IFS','lead':0},{'model':'GFS','lead':0}])
   self.assertEqual(len({(x['model'],x['lead']) for x in matrix}),82)

 def test_benchmark_has_no_cloud_publish_option(self):
  result=subprocess.run([sys.executable,str(ROOT/'scripts/measure_models.py'),'--help'],capture_output=True,text=True,check=True)
  self.assertNotIn('--mode ',result.stdout);self.assertNotIn('--bucket',result.stdout)

 def test_report_title_uses_measured_model(self):
  with tempfile.TemporaryDirectory() as folder:
   report=Path(folder)/'report.json'
   report.write_text(json.dumps({'status':'complete','run':'2026-11-14T00:00:00Z','processing':{'model':'GFS','profile':'full'},
      'processing_sha256':'test','plan':[],'mode':'discard','times':[]}))
   result=subprocess.run([sys.executable,str(ROOT/'scripts/summary.py'),str(report)],capture_output=True,text=True,check=True)
   self.assertTrue(result.stdout.startswith('# GFS 单批资源测试'))

if __name__=='__main__':unittest.main()
