import sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from clean_tiles import clean_feature,decode_rings,clean_lines,exclusive_bands
from shapely.geometry import Point

class GeometryTests(unittest.TestCase):
 def test_bands_remain_exclusive_for_translucent_rendering(self):
  a={'type':3,'geometry':[[[0,0],[20,0],[20,20],[0,20],[0,0]]],'tags':{'value':0}}
  b={'type':3,'geometry':[[[10,0],[30,0],[30,20],[10,20],[10,0]]],'tags':{'value':1}}
  fixed,overlap=exclusive_bands([a,b]);self.assertEqual(overlap,200)
  geoms={f['tags']['value']:decode_rings(f['geometry']) for f in fixed};self.assertEqual(geoms[0].intersection(geoms[1]).area,0)
  self.assertFalse(geoms[0].covers(Point(15,10)));self.assertTrue(geoms[1].covers(Point(15,10)))
 def test_quantized_zero_length_line_is_recorded_and_removed(self):
  feature={'type':2,'geometry':[[[20,30],[20,30]],[[1,2],[1,2],[3,4]]],'tags':{}}
  fixed,collapsed=clean_lines(feature);self.assertEqual(collapsed,1);self.assertEqual(fixed['geometry'],[[[1,2],[3,4]]])
 def test_overlapping_shells_preserve_union_instead_of_xor_hole(self):
  rings=[[[0,0],[20,0],[20,20],[0,20],[0,0]],[[10,0],[30,0],[30,20],[10,20],[10,0]]]
  fixed,metric=clean_feature({'type':3,'geometry':rings,'tags':{}});g=decode_rings(fixed['geometry'])
  self.assertTrue(g.is_valid);self.assertTrue(g.covers(Point(15,10)));self.assertEqual(g.area,600);self.assertEqual(metric['invalid_before'],1)
 def test_touching_hole_remains_unfilled(self):
  rings=[[[0,0],[30,0],[30,30],[0,30],[0,0]],[[0,10],[0,20],[10,20],[10,10],[0,10]]]
  fixed,_=clean_feature({'type':3,'geometry':rings,'tags':{}});g=decode_rings(fixed['geometry'])
  self.assertTrue(g.is_valid);self.assertFalse(g.covers(Point(5,15)));self.assertTrue(g.covers(Point(25,15)))
if __name__=='__main__':unittest.main()
