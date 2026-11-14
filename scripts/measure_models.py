"""双模式资源测试：只下载公开源，校验后丢弃成品，不提供云端发布选项。"""
import argparse,hashlib,json,subprocess,sys,time,shutil,fcntl
from pathlib import Path
import requests
from batch import ROOT,DiskMonitor,SafetyStop,build_plan,download,execute,parse_leads,source_fingerprint
from storage import digests
from freeze_models import validate_bundle

def definition(model,profile):
    raw=subprocess.check_output(['node','--input-type=module','-e',
        "import {catalog,fieldSources} from './src/catalog.mjs';const [profile,model]=process.argv.slice(1);console.log(JSON.stringify({products:catalog(profile,model),fields:fieldSources(profile,model)}))",
        profile,model],cwd=ROOT)
    return json.loads(raw)

def plan_sources(bundle,model,profile,leads):
    validate_bundle(bundle)
    spec=definition(model,profile)
    run,plan=build_plan(bundle['ecmwf_ifs025'],leads)
    domains=sorted({field['domain'] for field in spec['fields']})
    for domain in domains:
        needed={field['variable'] for field in spec['fields'] if field['domain']==domain}
        if not needed.issubset(set(bundle[domain].get('variables',[]))):raise ValueError(domain+'缺少必需变量')
    for item in plan:
        item['sources']=[{'domain':domain,'url':item['url'].replace('/ecmwf_ifs025/','/'+domain+'/')} for domain in domains]
    return run,plan,spec

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--model',choices=['IFS','GFS'],required=True)
    parser.add_argument('--metadata',type=Path,required=True)
    parser.add_argument('--work',type=Path,required=True);parser.add_argument('--report',type=Path,required=True)
    parser.add_argument('--leads',default='all');parser.add_argument('--profile',choices=['full','500hpa'],default='full')
    parser.add_argument('--product-workers',type=int,choices=range(1,5),default=4)
    parser.add_argument('--maxzoom',type=int,choices=range(6),default=5)
    parser.add_argument('--plan-only',action='store_true');args=parser.parse_args()
    bundle=json.loads(args.metadata.read_text())
    run,plan,spec=plan_sources(bundle,args.model,args.profile,parse_leads(args.leads))
    work=args.work.resolve();report_path=args.report.resolve()
    if report_path.is_relative_to(work):parser.error('报告必须在清理目录以外')
    work.mkdir(parents=True,exist_ok=True);report_path.parent.mkdir(parents=True,exist_ok=True)
    marker=work/'.dual-model-test-owner'
    if any(work.iterdir()) and not marker.exists():raise SafetyStop('临时目录不归本测试所有')
    lock=(work/'.lock').open('w');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    marker.write_text('public-dual-model-test-v1\n')
    processing={'model':args.model,'profile':args.profile,'maxzoom':args.maxzoom,'product_workers':args.product_workers,
        'source_code_sha256':source_fingerprint(),'version':'dual-model-discard-v1',
        'rain_accumulation_hours':3 if args.model=='IFS' else 1,
        'rain_note':'遵循当前所选空间源的原生累计时段；每3小时取一帧不等于合成3小时降水。'}
    report={'status':'planned','mode':'discard','run':run.isoformat(),'processing':processing,
        'processing_sha256':hashlib.sha256(json.dumps(processing,sort_keys=True,separators=(',',':')).encode()).hexdigest(),
        'plan':plan,'times':[],'source_metadata':bundle,
        'scope':'仅后台资源测试，不上传R2、不代表浏览器速度或视觉验收。'}
    def save():report_path.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    save()
    if args.plan_only:
        print(json.dumps({'model':args.model,'times':len(plan),'products':len(spec['products']),'fields':len(spec['fields'])}));return
    started=time.monotonic();monitor=DiskMonitor(work)
    try:
        with monitor,requests.Session() as session:
            report['status']='running';save()
            for item in plan:
                if time.monotonic()-started>5*3600:raise SafetyStop('测试总时长超过5小时')
                current=work/'current'
                if current.exists():shutil.rmtree(current)
                current.mkdir();fields=current/'fields';outputs=current/'output';lead=item['lead']
                step_start=time.monotonic();sources=[];source_fields=[];extract_seconds=0;regrid_seconds=0
                for source in item['sources']:
                    domain=source['domain'];native=current/domain;om=current/(domain+'.om')
                    metrics=download(source['url'],om,monitor,session)
                    names=[f['variable'] for f in spec['fields'] if f['domain']==domain]
                    extract_seconds+=execute([sys.executable,str(ROOT/'scripts/extract.py'),str(om),str(native),json.dumps(names),str(lead),domain],
                        monitor,report_path.parent/f'F{lead:03}-{domain}-extract.log',timeout=300)
                    entries=json.loads((native/'metadata.json').read_text())
                    source_fields.extend([{**row,'domain':domain} for row in entries]);om.unlink()
                    config=current/'regrid.json';config.write_text(json.dumps({'inputDir':str(native),'outputDir':str(fields),'domain':domain}))
                    regrid_seconds+=execute(['node',str(ROOT/'scripts/regrid.mjs'),str(config)],monitor,
                        report_path.parent/f'F{lead:03}-{domain}-regrid.log',timeout=300)
                    shutil.rmtree(native);sources.append({**source,**metrics});monitor.sample();monitor.check()
                job={'model':args.model,'fieldsDir':str(fields),'scratchDir':str(current/'scratch'),'outputDir':str(outputs),
                    'profile':args.profile,'lead':lead,'maxzoom':args.maxzoom,'python':sys.executable,'productWorkers':args.product_workers}
                config=current/'job.json';config.write_text(json.dumps(job))
                script='render-pool.mjs' if args.product_workers>1 else 'render.mjs'
                render_seconds=execute(['node','--max-old-space-size=4096',str(ROOT/'scripts'/script),str(config)],monitor,report_path.parent/f'F{lead:03}.log')
                rows=json.loads((outputs/'products.json').read_text())
                if [r['key'] for r in rows]!=[p['key'] for p in spec['products']]:raise ValueError('成品清单不完整')
                verify_start=time.monotonic()
                for row in rows:
                    if row['status']=='not_applicable':
                        if lead!=0 or '-rain-' not in row['key']:raise ValueError('禁止静默跳过产品')
                        continue
                    file=Path(row.pop('file'));sha,md5=digests(file)
                    if row['status']!='verified' or sha!=row['sha256'] or file.stat().st_size!=row['bytes']:raise ValueError('成品校验失败')
                    row['md5']=md5
                one={**item,'products':rows,'sources':sources,'source_fields':source_fields,
                    'product_bytes':sum(r.get('bytes',0) for r in rows),
                    'rain_accumulation_hours':None if lead==0 else processing['rain_accumulation_hours'],
                    'timings':{'download_seconds':sum(s['download_seconds'] for s in sources),'extraction_seconds':extract_seconds,
                        'regrid_seconds':regrid_seconds,'render_seconds':render_seconds,'pre_upload_verify_seconds':time.monotonic()-verify_start,
                        'storage_backend':'discard','product_upload_seconds':0,'time_until_ready_seconds':None}}
                monitor.sample();monitor.check();clean_start=time.monotonic();shutil.rmtree(current)
                one['timings']['cleanup_seconds']=time.monotonic()-clean_start
                one['timings']['time_total_seconds']=time.monotonic()-step_start
                report['times'].append(one);report['elapsed_seconds']=time.monotonic()-started;save()
                print(json.dumps({'model':args.model,'lead':lead,'bytes':one['product_bytes'],'seconds':one['timings']['time_total_seconds']}),flush=True)
            report['status']='complete'
    except Exception as error:
        report['status']='safety_stop' if isinstance(error,SafetyStop) else 'failed';report['error']=str(error)
        raise
    finally:
        report['disk']=monitor.metrics();report['elapsed_seconds']=time.monotonic()-started;save()

if __name__=='__main__':main()
