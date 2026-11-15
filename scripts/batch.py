"""IFS 全批处理入口：冻结起报、逐时次处理、磁盘保险丝、校验后发布。"""
from pathlib import Path
from datetime import datetime,timezone
import argparse,hashlib,json,os,re,shutil,signal,subprocess,sys,threading,time
import requests
from storage import PREFIX,Publisher,LocalStore,R2Store,SafetyStop,encoded,digests

ROOT=Path(__file__).resolve().parents[1]
from model_sources import BASE as SPATIAL,definition as product_definition,discover,model_plan,build_plan
SOURCE=SPATIAL+'ecmwf_ifs025/'
UTC=timezone.utc

def directory_bytes(root):
    total=0
    for folder,_,files in os.walk(root,followlinks=False):
        for name in files:
            path=Path(folder)/name
            try:
                if not path.is_symlink():total+=path.stat().st_size
            except FileNotFoundError:
                # 采样时生产子进程可能刚完成正常清理，不把这一竞争当磁盘错误。
                continue
    return total

class DiskMonitor:
    def __init__(self,work,min_free=2_000_000_000,max_work=8_000_000_000):
        self.work=work;self.min_free=min_free;self.max_work=max_work
        self.initial_free=shutil.disk_usage(work).free;self.lowest_free=self.initial_free;self.peak_work=0
        self.error=None;self.child=None;self.done=threading.Event();self.thread=None

    def sample(self):
        free=shutil.disk_usage(self.work).free
        # 目录扫描按两秒采样；不是对每个瓦片执行一次扫描。
        size=directory_bytes(self.work)
        self.lowest_free=min(self.lowest_free,free);self.peak_work=max(self.peak_work,size)
        if free<self.min_free:self.error='临时磁盘剩余空间低于保留量'
        if size>self.max_work:self.error='工作目录超过安全容量'
        if self.error and self.child and self.child.poll() is None:
            try:os.killpg(self.child.pid,signal.SIGTERM)
            except ProcessLookupError:pass

    def check(self):
        if self.error:raise SafetyStop(self.error)

    def __enter__(self):
        self.sample();self.check()
        def watch():
            while not self.done.wait(2):
                try:self.sample()
                except OSError as error:self.error=f'无法读取磁盘状态：{error}'
        self.thread=threading.Thread(target=watch,daemon=True);self.thread.start();return self

    def __exit__(self,*unused):
        self.done.set();self.thread.join();self.sample()
        if not unused[0]:self.check()

    def metrics(self):
        return {'initial_free_bytes':self.initial_free,'lowest_free_bytes':self.lowest_free,
            'observed_peak_work_bytes':self.peak_work,'sampling_seconds':2,
            'min_free_bytes':self.min_free,'max_work_bytes':self.max_work,
            'note':'工作目录为抽样峰值；不包含目录外依赖、操作系统和其他进程占用。磁盘剩余量包含同一文件系统所有占用。'}

def execute(command,monitor,log,timeout=1200):
    start=time.monotonic()
    with log.open('w') as stream:
        child=subprocess.Popen(command,stdout=stream,stderr=subprocess.STDOUT,start_new_session=True,cwd=ROOT)
        monitor.child=child
        try:
            while child.poll() is None:
                monitor.check()
                if time.monotonic()-start>timeout:raise SafetyStop('单时次处理超时')
                time.sleep(.2)
            monitor.check()
            if child.returncode:
                # 并行处理器失败时，清理同一进程组里的孙进程，不能留下几何任务继续占用CPU。
                try:os.killpg(child.pid,signal.SIGTERM)
                except ProcessLookupError:pass
                raise RuntimeError(f'处理阶段退出码 {child.returncode}；查看本地日志 {log.name}')
        finally:
            if child.poll() is None:
                os.killpg(child.pid,signal.SIGTERM)
                try:child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(child.pid,signal.SIGKILL);child.wait()
            monitor.child=None
    return time.monotonic()-start

def _download_once(url,path,monitor,session):
    start=time.monotonic();part=path.with_suffix('.part')
    with session.get(url,stream=True,timeout=(15,90)) as response:
        response.raise_for_status();expected=int(response.headers.get('Content-Length','0'))
        if not 0<expected<2_000_000_000:raise SafetyStop('源文件大小未知或过大')
        if shutil.disk_usage(path.parent).free<expected+monitor.min_free:
            raise SafetyStop('下载源文件将侵占磁盘保留空间')
        digest=hashlib.sha256();size=0
        with part.open('wb') as stream:
            for chunk in response.iter_content(1024*1024):
                monitor.check();stream.write(chunk);digest.update(chunk);size+=len(chunk)
                if size>expected:raise ValueError('源文件长度超出声明')
        if size!=expected:raise ValueError('源文件下载不完整')
        part.replace(path)
        return {'url':url,'bytes':size,'sha256':digest.hexdigest(),'etag':response.headers.get('ETag'),'last_modified':response.headers.get('Last-Modified'),
            'download_seconds':time.monotonic()-start}

def download(url,path,monitor,session):
    start=time.monotonic()
    for attempt in (1,2):
        try:
            result=_download_once(url,path,monitor,session)
            return {**result,'attempts':attempt,'download_seconds':time.monotonic()-start}
        except (requests.RequestException,ValueError):
            path.with_suffix('.part').unlink(missing_ok=True)
            if attempt==2:raise
            monitor.check();time.sleep(3)

def prepare_fields(item,spec,current,monitor,session,log_dir):
    fields=current/'fields'
    lead=item['lead']
    sources=[];source_fields=[];extract_seconds=0;regrid_seconds=0
    for source in item['sources']:
        domain=source['domain'];native=current/domain;om=current/(domain+'.om')
        metrics=download(source['url'],om,monitor,session)
        names=[f['variable'] for f in spec['fields'] if f['domain']==domain]
        extract_seconds+=execute([sys.executable,str(ROOT/'scripts/extract.py'),str(om),str(native),json.dumps(names),str(lead),domain],
            monitor,log_dir/f'F{lead:03}-{domain}-extract.log',timeout=300)
        entries=json.loads((native/'metadata.json').read_text())
        source_fields.extend([{**row,'domain':domain} for row in entries]);om.unlink()
        config=current/'regrid.json';config.write_text(json.dumps({'inputDir':str(native),'outputDir':str(fields),'domain':domain}))
        regrid_seconds+=execute(['node',str(ROOT/'scripts/regrid.mjs'),str(config)],monitor,
            log_dir/f'F{lead:03}-{domain}-regrid.log',timeout=300)
        shutil.rmtree(native);sources.append({**source,**metrics});monitor.sample();monitor.check()
    return sources,source_fields,extract_seconds,regrid_seconds

def parse_leads(value):
    if value=='all':return list(range(0,121,3))
    try:return [int(s) for s in value.split(',')]
    except ValueError as error:raise argparse.ArgumentTypeError('时次应为 all 或 0,3,6 等整数清单') from error

def valid_time_manifest(value,definition,lead,fingerprint):
    if value.get('processing_sha256')!=fingerprint:return False
    rows=value.get('products',[])
    if [r.get('key') for r in rows]!=[p['key'] for p in definition['products']]:
        raise SafetyStop('恢复用清单缺少产品，禁止跳过本时次')
    for row in rows:
        if row.get('status')=='not_applicable':
            if lead!=0 or '-rain-' not in row['key']:raise SafetyStop('恢复清单含异常跳过项')
        elif row.get('status')!='verified' or not re.fullmatch('[a-f0-9]{32}',row.get('md5','')) or row.get('bytes',0)<=0:
            raise SafetyStop('恢复清单校验信息异常')
    return True

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work',type=Path,required=True)
    parser.add_argument('--report',type=Path,required=True)
    parser.add_argument('--model',choices=['IFS','GFS'],help='独立发布指定模式；省略则沿用旧IFS测试入口')
    parser.add_argument('--mode',choices=['discard','local','r2'],default='discard')
    parser.add_argument('--output',type=Path)
    parser.add_argument('--bucket',default=os.environ.get('R2_BUCKET',''))
    parser.add_argument('--metadata',type=Path)
    parser.add_argument('--leads',default='all')
    parser.add_argument('--profile',choices=['full','500hpa'],default='full')
    parser.add_argument('--maxzoom',type=int,default=5)
    parser.add_argument('--product-workers',type=int,default=1)
    parser.add_argument('--plan-only',action='store_true')
    args=parser.parse_args()
    if not 0<=args.maxzoom<=5:parser.error('缩放层级只允许0至5')
    if not 1<=args.product_workers<=4:parser.error('产品并行数只允许1至4')
    work=args.work.resolve();report_path=args.report.resolve()
    if report_path.is_relative_to(work):parser.error('报告必须放在清理目录之外')
    if args.mode=='local' and not args.output:parser.error('local 模式需要 --output')
    if args.output and (args.output.resolve().is_relative_to(work) or work.is_relative_to(args.output.resolve())):
        parser.error('本地存储与临时工作目录不能互相包含')
    work.mkdir(parents=True,exist_ok=True);report_path.parent.mkdir(parents=True,exist_ok=True)
    marker=work/'.weather-map-batch-owner'
    if any(work.iterdir()) and not marker.exists():raise SafetyStop('工作目录非空且不归本程序所有')
    marker.write_text('open-weather-map-batch-v1\n')
    # 同一工作目录不能并发执行；远端另由 GitHub concurrency 和存储租约约束。
    import fcntl
    lock=(work/'.lock').open('w');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    session=requests.Session()
    model=args.model or 'IFS'
    prefix=PREFIX
    leads=parse_leads(args.leads)
    definition=product_definition(model,args.profile)
    if args.model:
        if args.mode=='r2' and not ((args.profile=='500hpa' and leads==[0]) or (args.profile=='full' and leads==list(range(0,121,3)))):
            parser.error('R2发布支持500hPa/F000首图或完整产品全时次')
        scope='full' if args.profile=='full' and leads==list(range(0,121,3)) else 'preview'
        prefix=f'weather-v1/{model.lower()}/{scope}/'
        if args.metadata:
            bundle=json.loads(args.metadata.read_text())
            candidates={domain:[meta] for domain,meta in bundle.items()}
        else:
            candidates=discover(definition,session)
        run,plan,meta=model_plan(definition,candidates,leads)
    else:
        if args.metadata:meta=json.loads(args.metadata.read_text())
        else:
            response=session.get(SOURCE+'latest.json',timeout=(15,60));response.raise_for_status();meta=response.json()
        run,plan=build_plan(meta,leads)
        for item in plan:
            item['sources']=[{'domain':'ecmwf_ifs025','url':item['url']}]
    processing={'model':model,'profile':args.profile,'maxzoom':args.maxzoom,'leads':[x['lead'] for x in plan],
        'products':definition['products'],'line_sigma':2,'rain_trace_sigma':0.6,'rain_display_subdivision':2,'cape_sigma':.5,'other_fill_sigma':2,
        'version':'weather-tiles-v1' if args.model else 'ifs-batch-v1','source_code_sha256':source_fingerprint()}
    processing['product_workers']=args.product_workers
    fingerprint=hashlib.sha256(encoded(processing)).hexdigest()
    run_id=run.strftime('%Y%m%d%H')+'-'+fingerprint[:12]
    report={'status':'planned','mode':args.mode,'run':run.isoformat(),'run_id':run_id,'processing_sha256':fingerprint,
        'processing':processing,'plan':plan,'source_metadata':meta,'times':[],
        'scope':'容量与运行稳定性测试；未构成业务平滑质量验收或实际宿主性能验收。',
        'fuse_scope':'R2 容量检查覆盖目标桶全部对象；批次数与上传操作预算仅管理本程序测试前缀。未限制其他桶、其他生产者及终端 B 类读取。'}
    def save():report_path.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    save()
    if args.plan_only:
        print(json.dumps({'status':'planned','run':report['run'],'times':len(plan),'products':len(definition['products']),'fields':len(definition['fields'])}));return
    monitor=DiskMonitor(work);publisher=None;lease=None;started=time.monotonic()
    try:
        with monitor:
            if args.mode!='discard':
                if args.mode=='local':
                    store=LocalStore(args.output,prefix=prefix)
                elif args.model:
                    store=model_store(model,prefix)
                else:
                    store=R2Store(args.bucket)
                lease=acquire_lease(store)
                publisher=Publisher(store)
                active=(publisher.available or {}).get('run_id','')
                if active[:10]>run_id[:10]:
                    raise SafetyStop('已有更新起报正在发布')
                if publisher.latest and publisher.latest['run']>report['run']:
                    raise SafetyStop('不允许以旧起报覆盖更新的完整资料')
                # ListObjects 属于A类，不可把所有读取都算作B类。按可能的分页数保守预留。
                max_objects=len(publisher.objects)+(len(definition['products'])+1)*len(plan)+20
                max_pages=max(1,(max_objects+999)//1000)
                reserved_a=2*(len(definition['products'])*len(plan)+len(plan)*2+30+(len(plan)+4)*max_pages)
                publisher.reserve_operations(datetime.now(UTC).date().isoformat(),reserved_a)
                publisher.prepare(run_id)
                publisher.put_json(prefix+f'runs/{run_id}/progress.json',{'run':report['run'],'processing_sha256':fingerprint,'processor_commit':os.environ.get('PROCESSOR_COMMIT'),'status':'incomplete'})
            report['status']='running';save()
            for item in plan:
                if time.monotonic()-started>5*3600:raise SafetyStop('整批任务接近时间上限')
                lead=item['lead'];time_key=prefix+f'runs/{run_id}/F{lead:03}/';manifest_key=time_key+'manifest.json'
                if publisher:
                    existing=publisher.store.read_json(manifest_key)
                    if existing and valid_time_manifest(existing,definition,lead,fingerprint) and all(publisher.matches(r['object_key'],r['bytes'],r['md5']) for r in existing['products'] if r['status']=='verified'):
                        publisher.publish_time(existing)
                        report['times'].append({**existing,'resumed':True});save();continue
                # 前次失败遗留也只清理本程序标记下的当前时次，不删除任何源项目文件。
                current=work/'current'
                if current.exists():shutil.rmtree(current)
                current.mkdir();outputs=current/'output';scratch=current/'scratch'
                report['current_lead']=lead;report['disk']=monitor.metrics();save()
                time_start=time.monotonic()
                sources,source_fields,extraction_seconds,regrid_seconds=prepare_fields(item,definition,current,monitor,session,report_path.parent)
                job={'model':model,'fieldsDir':str(current/'fields'),'scratchDir':str(scratch),'outputDir':str(outputs),
                    'profile':args.profile,'lead':lead,'maxzoom':args.maxzoom,'python':sys.executable,'productWorkers':args.product_workers}
                config=current/'job.json';config.write_text(json.dumps(job))
                render_script='render-pool.mjs' if args.product_workers>1 else 'render.mjs'
                render_seconds=execute(['node','--max-old-space-size=4096',str(ROOT/'scripts'/render_script),str(config)],monitor,
                    report_path.parent/f'F{lead:03}.log')
                rows=json.loads((outputs/'products.json').read_text())
                if [r['key'] for r in rows]!=[r['key'] for r in definition['products']]:
                    raise ValueError('生成清单缺少产品或顺序异常')
                files=[];verify_start=time.monotonic()
                for row in rows:
                    if row['status']=='not_applicable':
                        if lead!=0 or '-rain-' not in row['key']:raise ValueError('不允许静默跳过产品')
                        continue
                    path=Path(row['file']);sha,md5=digests(path)
                    if row['status']!='verified' or sha!=row['sha256'] or path.stat().st_size!=row['bytes']:
                        raise ValueError('上传前成品校验不一致')
                    row['md5']=md5;row['object_key']=time_key+path.name;files.append((row['object_key'],path))
                verify_seconds=time.monotonic()-verify_start;reserve_seconds=0
                if publisher:
                    reserve_start=time.monotonic();publisher.reserve_time(files);reserve_seconds=time.monotonic()-reserve_start
                upload_start=time.monotonic()
                for row in rows:
                    if row['status']!='verified':continue
                    if publisher and not publisher.matches(row['object_key'],row['bytes'],row['md5']):
                        publisher.put(row['object_key'],Path(row['file']),immutable=True)
                    row.pop('file',None)
                upload_seconds=time.monotonic()-upload_start
                one={**item,'model':model,'run':report['run'],'profile':args.profile,'processing_sha256':fingerprint,'products':rows,'source':sources[0],'sources':sources,'source_fields':source_fields,
                    'product_bytes':sum(r.get('bytes',0) for r in rows),'rain_accumulation_hours':None if lead==0 else (3 if model=='IFS' else 1)}
                one['timings']={'regrid_seconds':regrid_seconds,'download_seconds':sum(s['download_seconds'] for s in sources),'extraction_seconds':extraction_seconds,
                    'render_seconds':render_seconds,'pre_upload_verify_seconds':verify_seconds,
                    'storage_backend':args.mode,'storage_snapshot_and_reservation_seconds':reserve_seconds,'product_upload_seconds':upload_seconds if publisher else 0}
                publish_start=time.monotonic()
                if publisher:
                    publisher.put_json(manifest_key,one)
                    publisher.publish_time(one)
                one['timings']['catalog_publish_seconds']=time.monotonic()-publish_start if publisher else 0
                one['timings']['time_until_ready_seconds']=time.monotonic()-time_start if publisher else None
                report['times'].append(one);monitor.sample();monitor.check()
                report['disk']=monitor.metrics();report['elapsed_seconds']=time.monotonic()-started;save()
                cleanup_start=time.monotonic();shutil.rmtree(current)
                one['timings']['cleanup_seconds']=time.monotonic()-cleanup_start
                one['timings']['time_total_seconds']=time.monotonic()-time_start;save()
                print(json.dumps({'lead':lead,'status':'verified','products':len(files),'bytes':one['product_bytes'],'work_bytes_after_cleanup':directory_bytes(work)}),flush=True)
            if len(report['times'])!=len(plan):raise ValueError('整批时次未全部完成')
            if publisher:
                manifest={'run_id':run_id,'run':report['run'],'processing_sha256':fingerprint,'status':'complete',
                    'profile':args.profile,'times':[{'lead':t['lead'],'valid':t['valid'],'product_bytes':t['product_bytes']} for t in report['times']],
                    'product_bytes':sum(t['product_bytes'] for t in report['times']),'model':model,'attribution':f"Open-Meteo / {'ECMWF' if model=='IFS' else 'NOAA'}, CC BY 4.0"}
                publisher.finish_run(manifest,[t['lead'] for t in plan])
                report['store']={'observed_peak_bytes':publisher.peak_bytes,'final_tracked_bytes':publisher.current_bytes,
                    'batch_count':len(publisher.batches),'application_requests':publisher.store.requests,
                    'note':'SDK 自动重试次数不在 application_requests 中；A 类预算按每次调用最多两次尝试预留。'}
            report['status']='complete'
    except Exception as error:
        report['status']='safety_stop' if isinstance(error,SafetyStop) else 'failed'
        # SDK 错误可能包含账户信息；公开日志只保留错误类型，详细错误不打印凭据。
        code=getattr(error,'response',{}).get('Error',{}).get('Code','')
        safe_code=code if re.fullmatch('[A-Za-z0-9_.-]{1,60}',code) else ''
        report['error']=str(error) if not args.mode=='r2' or isinstance(error,SafetyStop) else type(error).__name__+(':'+safe_code if safe_code else '')
        raise
    finally:
        release_error=None
        if lease is not None:
            try:release_lease(store,lease)
            except Exception as error:
                release_error=error;report['status']='safety_stop';report['lease_release_error']=type(error).__name__
        report['disk']=monitor.metrics();report['elapsed_seconds']=time.monotonic()-started;save()
        if release_error:raise SafetyStop('写入租约未正常释放；停止后续生产并保留资料') from release_error

def model_store(model,prefix):
    accounts=[os.environ.get(name+'_R2_ACCOUNT_ID','') for name in ('IFS','GFS')]
    if not all(re.fullmatch('[a-f0-9]{32}',v) for v in accounts) or accounts[0]==accounts[1]:
        raise SafetyStop('需要两个不同的R2账户，仅向所选模式的桶发布')
    credentials={key:os.environ.get(model+'_R2_'+suffix,'') for key,suffix in (
        ('R2_ACCOUNT_ID','ACCOUNT_ID'),('AWS_ACCESS_KEY_ID','ACCESS_KEY_ID'),('AWS_SECRET_ACCESS_KEY','SECRET_ACCESS_KEY'))}
    return R2Store(os.environ.get(model+'_R2_BUCKET',''),prefix=prefix,credentials=credentials)

def source_fingerprint():
    digest=hashlib.sha256()
    for folder,extensions in [('src',{'.mjs'}),('scripts',{'.py','.mjs'})]:
        for path in sorted((ROOT/folder).iterdir()):
            if path.suffix in extensions:digest.update(path.name.encode()+b'\0'+path.read_bytes())
    return digest.hexdigest()

def acquire_lease(store):
    """七小时排他租约；任务本身最多五小时，异常退出时不与下一任务竞争。"""
    import uuid
    token=uuid.uuid4().hex;payload={'token':token,'expires':time.time()+7*3600}
    key=PREFIX+'writer-lock.json'
    if isinstance(store,LocalStore):
        import fcntl
        stream=(store.root/'.writer.lock').open('w')
        try:fcntl.flock(stream,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:
            stream.close();raise SafetyStop('已有生产任务持有写入锁')
        return stream
    from botocore.exceptions import ClientError
    try:
        store.requests['get']+=1;response=store.client.get_object(Bucket=store.bucket,Key=key)
        with response['Body'] as body:previous=json.loads(body.read())
        if previous['expires']>time.time():raise SafetyStop('已有生产任务持有未到期写入租约')
        condition={'IfMatch':response['ETag']}
    except ClientError as error:
        if error.response['Error']['Code'] not in ('NoSuchKey','404'):raise
        condition={'IfNoneMatch':'*'}
    try:
        store.requests['put']+=1
        store.client.put_object(Bucket=store.bucket,Key=key,Body=encoded(payload),ContentType='application/json',**condition)
    except ClientError as error:
        raise SafetyStop('未能独占生产租约，禁止并发上传') from error
    return token

def release_lease(store,lease):
    if isinstance(store,LocalStore):lease.close();return
    current=store.read_json(PREFIX+'writer-lock.json')
    if not current or current.get('token')!=lease or current.get('expires',0)<=time.time():
        raise SafetyStop('写入租约归属或有效期异常')
    store.delete([PREFIX+'writer-lock.json'])

if __name__=='__main__':
    try:main()
    except Exception as error:
        # 详细公开错误已由报告脱敏；不把 SDK 响应和环境变量直接输出。
        print(f'任务停止：{type(error).__name__}',file=sys.stderr);sys.exit(2)
