"""固定同一公开起报做版本比较；不访问网络、不静默切换输入。"""
import json,os,subprocess,sys,tempfile,unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]

class FrozenBenchmark(unittest.TestCase):
 def run_freeze(self,metadata):
  with tempfile.TemporaryDirectory() as folder:
   path=Path(folder)/'metadata.json';output=Path(folder)/'output.txt'
   path.write_text(json.dumps(metadata))
   result=subprocess.run([sys.executable,str(ROOT/'scripts/freeze_run.py'),'--metadata',str(path)],
       env={**os.environ,'GITHUB_OUTPUT':str(output)},capture_output=True,text=True,timeout=10)
   return result,output.read_text() if output.exists() else ''

 def test_original_41_step_run_is_preserved(self):
  meta=json.loads((ROOT/'benchmarks/ifs-20261113T18Z.json').read_text());result,output=self.run_freeze(meta)
  self.assertEqual(result.returncode,0,result.stderr)
  rows=dict(line.split('=',1) for line in output.splitlines())
  self.assertEqual(json.loads(rows['metadata']),meta)
  self.assertEqual(json.loads(rows['matrix'])['lead'],list(range(0,121,3)))

 def test_incomplete_run_cannot_replace_frozen_inputs(self):
  meta=json.loads((ROOT/'benchmarks/ifs-20261113T18Z.json').read_text());meta['completed']=False
  result,output=self.run_freeze(meta);self.assertNotEqual(result.returncode,0);self.assertEqual(output,'')

 def test_missing_forecast_step_stops_before_matrix_is_written(self):
  meta=json.loads((ROOT/'benchmarks/ifs-20261113T18Z.json').read_text());meta['valid_times']=meta['valid_times'][:-10]
  result,output=self.run_freeze(meta);self.assertNotEqual(result.returncode,0);self.assertEqual(output,'')

if __name__=='__main__':unittest.main()
