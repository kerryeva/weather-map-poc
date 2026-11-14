"""按真实目录覆盖全部几何产品；用独立几何库检查生成器输出。"""
import json,subprocess,sys,unittest
from pathlib import Path
from shapely.geometry import shape,Point
from shapely import union_all

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'scripts'))
from clean_source import clean_source

class AllProductGeometry(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  script=r"""
import {catalog,STYLES,colorFor} from './src/catalog.mjs';
import {lineContours} from './src/lines.mjs';
import {fillBands} from './src/fill.mjs';
const products=catalog(),cases=[];
for(const product of products){
 if(product.kind==='wind')continue;
 if(product.kind==='line'){
  const interval=STYLES[product.id].interval,threshold=interval*4;
  const grid={nx:9,ny:9,lonMin:-87,latMin:-14,dx:.25,dy:.25};
  for(const scenario of ['constant','near_threshold','nonzero_short']){
   const values=new Float64Array(81).fill(threshold-interval);
   if(scenario!=='constant'){
    values[40]=threshold+(scenario==='near_threshold'?Number.EPSILON*Math.abs(threshold):1e-9);
    values[60]=threshold+interval;
   }
   cases.push({key:product.key,kind:'line',scenario,geo:lineContours(values,grid,interval)});
  }
 }else{
  const grid={nx:8,ny:5,lonMin:-180,latMin:-90,dx:45,dy:45};
  const [first,second]=STYLES[product.id].breaks;
  for(const scenario of ['threshold_constant','isolated_threshold','missing_cell'])for(const threshold of scenario==='missing_cell'?[first]:STYLES[product.id].breaks){
   // 与实际原生字段/派生场一致使用Float32；-0.1等十进制阈值不能在其中精确表示。
   const values=new Float32Array(40).fill(scenario==='isolated_threshold'?threshold-1:scenario==='missing_cell'?(first+second)/2:threshold);
   if(scenario==='isolated_threshold')values[19]=threshold;
   if(scenario==='missing_cell')values[19]=NaN;
   const sigma=product.id==='rain'?0:product.id==='cape'?.5:2;
   cases.push({key:product.key,kind:'fill',scenario,threshold,representedValue:values[0],expectedColor:colorFor(product.id,values[0]),geo:fillBands(product.id,values,grid,{sigma})});
  }
 }
}
console.log(JSON.stringify({products,cases,fillThresholdCases:products.filter(p=>p.kind==='fill').reduce((sum,p)=>sum+2*STYLES[p.id].breaks.length+1,0)}));
"""
  result=subprocess.run(['node','--input-type=module','-e',script],cwd=ROOT,text=True,capture_output=True,check=True,timeout=30)
  cls.data=json.loads(result.stdout)

 def test_catalog_coverage_has_no_missing_product(self):
  products=self.data['products'];self.assertEqual(len(products),54)
  self.assertEqual(sum(p['kind']=='line' for p in products),14)
  self.assertEqual(sum(p['kind']=='fill' for p in products),33)
  expected={p['key'] for p in products if p['kind']!='wind'}
  self.assertEqual({c['key'] for c in self.data['cases']},expected)
  self.assertEqual(len(self.data['cases']),14*3+self.data['fillThresholdCases'])

 def test_all_line_and_fill_products_pass_source_geometry_validation(self):
  for case in self.data['cases']:
   with self.subTest(product=case['key'],scenario=case['scenario'],threshold=case.get('threshold')):
    geo=case['geo'];report=clean_source(geo);self.assertEqual(report['invalid_after'],0)
    geometries=[]
    for feature in geo['features']:
     geometry=shape(feature['geometry']);self.assertTrue(geometry.is_valid)
     if case['kind']=='line':self.assertGreater(geometry.length,0)
     elif not geometry.is_empty:geometries.append(geometry)
    if geometries:
     overlap=sum(g.area for g in geometries)-union_all(geometries).area
     self.assertLessEqual(overlap,1e-7,'源填色分级不能发生面积重叠')

 def test_invalid_line_still_stops_instead_of_being_silently_repaired(self):
  data={'type':'FeatureCollection','features':[{'type':'Feature','properties':{'value':-52},'geometry':{'type':'LineString','coordinates':[[-86,-13],[-86,-13]]}}]}
  with self.assertRaisesRegex(ValueError,'非面源几何无效'):clean_source(data)

 def test_constant_thresholds_match_legend_and_missing_cells_stay_unfilled(self):
  for case in self.data['cases']:
   if case['kind']!='fill' or case['scenario']=='isolated_threshold':continue
   with self.subTest(product=case['key'],scenario=case['scenario'],threshold=case['threshold']):
    features=case['geo']['features'];combined=union_all([shape(f['geometry']) for f in features])
    if case['scenario']=='missing_cell':
     self.assertFalse(combined.covers(Point(-45,0)),'不能填平原场缺测格点')
    elif case['expectedColor'] is None:self.assertEqual(features,[])
    else:
     self.assertTrue(combined.covers(Point(0,0)))
     self.assertEqual({f['properties']['color'] for f in features},{case['expectedColor']})

 def test_collapsed_source_surface_is_counted(self):
  data={'type':'FeatureCollection','features':[{'type':'Feature','properties':{'value':1},'geometry':{'type':'Polygon','coordinates':[[[0,0],[1,0],[2,0],[0,0]]]}}]}
  report=clean_source(data);self.assertEqual(report['collapsed_to_empty'],1)
  self.assertTrue(shape(data['features'][0]['geometry']).is_empty)

if __name__=='__main__':unittest.main()
