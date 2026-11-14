"""冻结一次已完整发布的IFS起报，供所有独立时次任务共同使用。"""
from datetime import datetime,timedelta,timezone
import json,os,urllib.request

url='https://openmeteo.s3.amazonaws.com/data_spatial/ecmwf_ifs025/latest.json'
with urllib.request.urlopen(url,timeout=60) as response:meta=json.load(response)
run=datetime.fromisoformat(meta['reference_time'].replace('Z','+00:00')).astimezone(timezone.utc)
if not meta.get('completed') or run.hour not in (0,6,12,18) or run.minute or run.second:
    raise SystemExit('起报未完成或目录格式异常')
times={datetime.fromisoformat(t.replace('Z','+00:00')) for t in meta['valid_times']}
leads=list(range(0,121,3))
if any(run+timedelta(hours=i) not in times for i in leads):raise SystemExit('源目录尚未提供完整41时次')
print('Frozen public IFS run:',run.isoformat(),'leads:',len(leads))
with open(os.environ['GITHUB_OUTPUT'],'a') as stream:
    stream.write('metadata='+json.dumps(meta,separators=(',',':'))+'\n')
    stream.write('matrix='+json.dumps({'lead':leads},separators=(',',':'))+'\n')
