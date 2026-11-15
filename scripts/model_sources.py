"""按产品选择源域。两个模式独立发现起报，同一产品的所有输入必须同起报。"""
import json,subprocess
from datetime import datetime,timedelta,timezone
UTC=timezone.utc
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
BASE='https://openmeteo.s3.amazonaws.com/data_spatial/'

def stamp(value):
    value=datetime.fromisoformat(value.replace('Z','+00:00'))
    if value.tzinfo is None:raise ValueError('时间必须包含时区')
    return value.astimezone(UTC)

def build_plan(meta,leads,domain="ecmwf_ifs025",require_complete=True):
    run=stamp(meta['reference_time'])
    if (require_complete and not meta.get('completed')) or run.hour not in (0,6,12,18) or run.minute or run.second:
        raise ValueError('源起报尚未完整发布或起报时间异常')
    available={stamp(t) for t in meta['valid_times']}
    result=[]
    if not leads or len(set(leads))!=len(leads):raise ValueError('时次清单为空或重复')
    for lead in leads:
        if not isinstance(lead,int) or lead<0 or lead>120 or lead%3:
            raise ValueError('只允许0至120小时、每3小时间隔')
        valid=run+timedelta(hours=lead)
        if valid not in available:raise ValueError(f'源目录缺少 F{lead:03}')
        result.append({'lead':lead,'valid':valid.isoformat().replace('+00:00','Z'),
            'url':BASE+domain+'/'+run.strftime('%Y/%m/%d/%H%MZ/')+valid.strftime('%Y-%m-%dT%H%M.om')})
    return run,result

def definition(model,profile):
    raw=subprocess.check_output(['node','--input-type=module','-e',
        "import {catalog,fieldSources} from './src/catalog.mjs';const [profile,model]=process.argv.slice(1);console.log(JSON.stringify({products:catalog(profile,model),fields:fieldSources(profile,model)}))",
        profile,model],cwd=ROOT)
    return json.loads(raw)


def model_plan(spec,candidates,leads):
    by_domain={}
    for domain in sorted({f['domain'] for f in spec['fields']}):
        needed={f['variable'] for f in spec['fields'] if f['domain']==domain}
        if leads==[0]:
            needed.discard('precipitation')
        by_domain[domain]={}
        for meta in candidates[domain]:
            if not needed.issubset(meta['variables']):
                continue
            try:
                run,plan=build_plan(meta,leads,domain=domain,require_complete=False)
            except ValueError:
                continue
            by_domain[domain][run]=(meta,plan)
    common=set.intersection(*(set(runs) for runs in by_domain.values()))
    if not common:
        raise ValueError('所选模式的源域尚无同时覆盖变量和请求时次的共同起报')
    run=max(common)
    selected={domain:runs[run][0] for domain,runs in by_domain.items()}
    plans=[(domain,runs[run][1]) for domain,runs in by_domain.items()]
    plan=[]
    for index,base in enumerate(plans[0][1]):
        sources=[{'domain':domain,'url':items[index]['url']} for domain,items in plans]
        plan.append({**base,'sources':sources})
    return run,plan,selected

def discover(spec,session):
    candidates={}
    for domain in sorted({f['domain'] for f in spec['fields']}):
        candidates[domain]=[]
        for name in ('latest','in-progress'):
            response=session.get(BASE+domain+'/'+name+'.json',timeout=(15,60))
            if response.status_code==404 and name=='in-progress':
                continue
            response.raise_for_status()
            candidates[domain].append(response.json())
    return candidates
