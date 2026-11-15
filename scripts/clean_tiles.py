"""在 MVT 整数坐标中修复量化退化，修复后逐环验证；不在经纬度中盲目 buffer(0)。"""
from pathlib import Path
import json,sys,time,resource
from shapely import make_valid,set_precision,union_all
from shapely.geometry import Polygon,MultiPolygon,GeometryCollection
from shapely.geometry.polygon import orient
from shapely.ops import polygonize
from shapely.prepared import prep

def area(ring):
 return sum(a[0]*b[1]-b[0]*a[1] for a,b in zip(ring,ring[1:]))/2

def polygons(geometry):
 if geometry.is_empty:return []
 if geometry.geom_type=='Polygon':return [geometry]
 if hasattr(geometry,'geoms'):return [p for g in geometry.geoms for p in polygons(g)]
 return []

def decode_rings(rings,counts=None):
 parts=[];current=[]
 for r in rings:
  if len(r)<4 or area(r)==0:
   if counts is not None:counts['degenerate_rings_removed']+=1
   continue
  if area(r)>0:
   if current:parts.append(Polygon(current[0],current[1:]))
   current=[r]
  else:
   if not current:raise ValueError('MVT 孔洞缺少外环，不能猜测连接')
   current.append(r)
 if current:parts.append(Polygon(current[0],current[1:]))
 return MultiPolygon(parts) if len(parts)>1 else parts[0] if parts else GeometryCollection()

def clean_feature(feature):
 counts={'degenerate_rings_removed':0}
 before=decode_rings(feature['geometry'],counts);was_valid=before.is_valid
 # geojson-vt 已给定内外环。结构法合并相交外环、扣除孔洞；
 # linework 的奇偶规则会把重复缓冲区域当洞，造成大片漏填。
 fixed=make_valid(before,method='structure',keep_collapsed=False) if not was_valid else before
 parts=polygons(fixed);surface=MultiPolygon(parts) if len(parts)>1 else parts[0] if parts else GeometryCollection()
 # 量化交点也必须回到整数网格；采用保持有效性的精度缩减。
 snapped=set_precision(surface,1,mode='valid_output')
 if not snapped.is_valid:raise ValueError('瓦片修复后仍无效')
 rings=[]
 for polygon in polygons(snapped):
  polygon=orient(polygon,sign=1)
  rings.extend([[[int(round(x)),int(round(y))] for x,y in ring.coords] for ring in [polygon.exterior,*polygon.interiors]])
 candidate={**feature,'geometry':rings}
 if not decode_rings(rings).is_valid:raise ValueError('整数编码前复核失败')
 return candidate,{'invalid_before':int(not was_valid),'area_before':before.area,'area_after':snapped.area,'absolute_area_change':abs(snapped.area-before.area),'collapsed_to_empty':int(not before.is_empty and snapped.is_empty),**counts}

def clean_lines(feature):
 lines=[];collapsed=0
 for line in feature['geometry']:
  unique=[]
  for point in line:
   if not unique or point!=unique[-1]:unique.append(point)
  if len(unique)<2:collapsed+=1
  else:lines.append(unique)
 return {**feature,'geometry':lines},collapsed

def exclusive_bands(features):
 # 用实际将编码的坐标检查重叠；这里不能再次吸附后检查另一份几何。
 indexed=[(i,f,decode_rings(f['geometry'])) for i,f in enumerate(features) if f['type']==3]
 if len(indexed)<2:return features,0
 combined=union_all([g for _,_,g in indexed],grid_size=0)
 overlap=max(0,sum(g.area for _,_,g in indexed)-combined.area)
 if overlap<1e-8:return features,0
 # 独立简化/量化会破坏色带的共同边界。将全部边界一次性吸附、
 # 结点化并重建平面小面，再按原分级归属分配，避免逐面求差反复吸附。
 network=union_all([g.boundary for _,_,g in indexed],grid_size=1)
 owners=[(i,prep(g)) for i,f,g in sorted(indexed,key=lambda v:v[1]['tags']['value'],reverse=True)]
 assigned={i:[] for i,_,_ in indexed}
 for face in polygonize(list(network.geoms) if hasattr(network,'geoms') else [network]):
  point=face.representative_point()
  for index,geometry in owners:
   if geometry.covers(point):assigned[index].append(face);break
 replacements={}
 for index,feature,_ in indexed:
  clipped=union_all(assigned[index],grid_size=0)
  if not clipped.is_valid:raise ValueError('共同边界重建后无效')
  rings=[]
  for polygon in polygons(clipped):
   polygon=orient(polygon,sign=1)
   rings.extend([[[int(round(x)),int(round(y))] for x,y in r.coords] for r in [polygon.exterior,*polygon.interiors]])
  replacements[index]={**feature,'geometry':rings}
 result=[replacements.get(i,f) for i,f in enumerate(features) if i not in replacements or replacements[i]['geometry']]
 checked=[decode_rings(f['geometry']) for f in result if f['type']==3]
 if sum(g.area for g in checked)-union_all(checked,grid_size=0).area>1e-6:raise ValueError('分级面仍有面积重叠')
 return result,overlap

def main():
 start=time.process_time()
 source=Path(sys.argv[1]);dest=Path(sys.argv[2]);report={'method':'structure_precision_1_shared_noded_boundaries_nonzero_lines','features':0,'invalid_before':0,'invalid_after':0,'absolute_area_change':0,'collapsed_to_empty':0,'collapsed_line_parts':0,'degenerate_rings_removed':0,'overlap_tiles_fixed':0,'overlap_area_before_partition':0,'area_note':'无效输入的 area 是代数值，变化量不能直接解释为实际面积损失；需源填色/瓦片独立抽样比较'}
 with source.open() as fi,dest.open('w') as fo:
  for line in fi:
   tile=json.loads(line);features=[]
   for feature in tile['features']:
    if feature['type']==2:
     cleaned,collapsed=clean_lines(feature);report['collapsed_line_parts']+=collapsed
     if cleaned['geometry']:features.append(cleaned)
     continue
    if feature['type']!=3:features.append(feature);continue
    cleaned,metric=clean_feature(feature);report['features']+=1
    for key in ('invalid_before','absolute_area_change','collapsed_to_empty','degenerate_rings_removed'):report[key]+=metric[key]
    if cleaned['geometry']:features.append(cleaned)
   features,overlap=exclusive_bands(features);report['overlap_tiles_fixed']+=int(overlap>0);report['overlap_area_before_partition']+=overlap
   tile['features']=features;fo.write(json.dumps(tile,separators=(',',':'))+'\n')
 report['cpu_seconds']=time.process_time()-start;report['peak_rss_bytes']=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024)
 dest.with_suffix('.metrics.json').write_text(json.dumps(report,indent=2));print(json.dumps(report))

if __name__=='__main__':main()
