"""修复源分级面的点接触等退化，并保留前后变化证据。"""
from pathlib import Path
import json,sys,time,resource
from shapely import make_valid
from shapely.geometry import shape,mapping,MultiPolygon
from shapely.validation import explain_validity
from clean_tiles import polygons
def clean_source(data):
 report={'invalid_before':0,'invalid_after':0,'collapsed_to_empty':0,'repairs':[]}
 for i,f in enumerate(data['features']):
  g=shape(f['geometry'])
  if g.is_valid:continue
  report['invalid_before']+=1
  if g.geom_type not in ('Polygon','MultiPolygon'):raise ValueError('非面源几何无效，需要单独修复')
  fixed=make_valid(g,method='structure',keep_collapsed=False);parts=polygons(fixed);result=MultiPolygon(parts)
  if not result.is_valid:raise ValueError('源填色修复后仍无效')
  report['collapsed_to_empty']+=int(not g.is_empty and result.is_empty)
  report['repairs'].append({'feature':i,'reason':explain_validity(g),'area_before_algebraic':g.area,'area_after':result.area});f['geometry']=mapping(result)
 return report

def main():
 start=time.process_time();p=Path(sys.argv[1]);data=json.loads(p.read_text());report=clean_source(data)
 p.write_text(json.dumps(data,separators=(',',':')))
 report['cpu_seconds']=time.process_time()-start;report['peak_rss_bytes']=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024)
 p.with_suffix('.source-check.json').write_text(json.dumps(report,indent=2));print(json.dumps(report))

if __name__=='__main__':main()
