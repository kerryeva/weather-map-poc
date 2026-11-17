"""IFS 全批处理入口：冻结起报、逐时次处理、磁盘保险丝、校验后发布。"""
from pathlib import Path
from datetime import datetime,timezone
import argparse,hashlib,json,os,re,shutil,signal,subprocess,sys,threading,time
import requests
from storage import PREFIX,Publisher,LocalStore,R2Store,SafetyStop,encoded,digests,shard_budget,retained_bytes

ROOT=Path(__file__).resolve().parents[1]
from model_sources import BASE as SPATIAL,definition as product_definition,discover,model_plan,build_plan
SOURCE=SPATIAL+'ecmwf_ifs025/'
UTC=timezone.utc
FULL_LEADS=list(range(0,97,3))
STOP_DETAIL={}
# 四片并行后每片只有 9 个时次、5 小时上限摊下来每时次约 33 分钟，阶段预算可以按实测放宽：
# 抽取、重网格各 10 分钟，渲染 20 分钟；单时次总预算 25 分钟，9 个时次最多 3.75 小时，
# 仍留出整批熔断（5 小时）的余量。超时不再当作安全停止，而是重试一次后跳过该时次。
STAGE_BUDGETS={'extract':600,'regrid':600,'render':1200}
STAGE_RETRY_FACTOR=1.5
TIMESTEP_BUDGET=1500

class StageTimeout(SafetyStop):
    """单个处理阶段超时：可换更宽松的预算重试一次；仍超时则跳过本时次，不拖死整批。"""

class StageFailure(SafetyStop):
    """单个处理阶段非零退出：同样重试一次，仍失败就跳过本时次，让其余时次照常发布。"""

MAX_SKIPS=2

def shard_leads(index,count):
    """把完整时次表切成等长的连续分片，最后一片可能更短。"""
    if not 1<=count<=16 or not 0<=index<count:raise SafetyStop('分片参数只允许序号小于总数且总数不超过16')
    chunk=-(-len(FULL_LEADS)//count)
    return FULL_LEADS[index*chunk:(index+1)*chunk]

def public_error(error,mode):
    """公开日志只保留可安全展示的原因：SafetyStop 的中文说明，或异常类型加 SDK 错误码。"""
    code=getattr(error,'response',{}).get('Error',{}).get('Code','')
    safe_code=code if isinstance(code,str) and re.fullmatch('[A-Za-z0-9_.-]{1,60}',code) else ''
    if isinstance(error,SafetyStop) or mode!='r2':return str(error)
    return type(error).__name__+(':'+safe_code if safe_code else '')

def step_summary(lines):
    """关键结论写入 GitHub job summary；本地运行没有该环境变量时静默跳过。"""
    path=os.environ.get('GITHUB_STEP_SUMMARY')
    if not path:return
    try:
        with open(path,'a',encoding='utf-8') as stream:stream.write('\n'.join(lines)+'\n')
    except OSError:
        # 摘要写不进去不影响生产，只失去可读性。
        pass

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

def log_tail(log,lines=12,width=240):
    """失败时把子进程日志的末尾带回标准输出：否则云端只看到「退出码 1」，无从判断原因。"""
    try: rows=log.read_text(errors='replace').splitlines()[-lines:]
    except OSError: return []
    return [row[:width] for row in rows]

def execute(command,monitor,log,timeout=1200,label='处理阶段'):
    start=time.monotonic()
    with log.open('w') as stream:
        child=subprocess.Popen(command,stdout=stream,stderr=subprocess.STDOUT,start_new_session=True,cwd=ROOT)
        monitor.child=child
        try:
            while child.poll() is None:
                monitor.check()
                if time.monotonic()-start>timeout:
                    raise StageTimeout(f'{label}超时：已用{int(time.monotonic()-start)}秒，预算{timeout}秒')
                time.sleep(.2)
            monitor.check()
            if child.returncode:
                # 并行处理器失败时，清理同一进程组里的孙进程，不能留下几何任务继续占用CPU。
                try:os.killpg(child.pid,signal.SIGTERM)
                except ProcessLookupError:pass
                for row in log_tail(log):print('  | '+row,flush=True)
                raise StageFailure(f'{label}退出码 {child.returncode}')
        finally:
            if child.poll() is None:
                os.killpg(child.pid,signal.SIGTERM)
                try:child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(child.pid,signal.SIGKILL);child.wait()
            monitor.child=None
    return time.monotonic()-start

def execute_stage(command,monitor,log,budget,label,reset=None,deadline=None):
    """阶段超时按 1.5 倍预算重试一次；仍超时抛 StageTimeout，由主循环跳过本时次。

    每个阶段实际可用时间还会被「本时次剩余预算」截断，因此放宽阶段预算不会超过
    整批熔断，也不会让一个坏时次吃掉后面时次的时间。"""
    for attempt,seconds in enumerate((budget,int(budget*STAGE_RETRY_FACTOR)),1):
        allowance=seconds
        if deadline is not None:allowance=min(seconds,max(0,deadline-time.monotonic()))
        if allowance<1:raise StageTimeout(f'{label}超时：本时次预算已用完')
        try:
            return execute(command,monitor,log,timeout=int(allowance),label=f'{label}（第{attempt}次）')
        except (StageTimeout,StageFailure):
            if attempt>1:raise
            # 重试用干净目录，避免上一次的半个输出被当成成品。
            if reset is not None:reset()
            monitor.sample();monitor.check()

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

def prepare_fields(item,spec,current,monitor,session,log_dir,deadline=None):
    fields=current/'fields'
    lead=item['lead']
    sources=[];source_fields=[];extract_seconds=0;regrid_seconds=0
    for source in item['sources']:
        domain=source['domain'];native=current/domain;om=current/(domain+'.om')
        metrics=download(source['url'],om,monitor,session)
        names=[f['variable'] for f in spec['fields'] if f['domain']==domain]
        extract_seconds+=execute_stage([sys.executable,str(ROOT/'scripts/extract.py'),str(om),str(native),json.dumps(names),str(lead),domain],
            monitor,log_dir/f'F{lead:03}-{domain}-extract.log',STAGE_BUDGETS['extract'],'抽取',
            reset=lambda: shutil.rmtree(native,ignore_errors=True),deadline=deadline)
        entries=json.loads((native/'metadata.json').read_text())
        source_fields.extend([{**row,'domain':domain} for row in entries]);om.unlink()
        config=current/'regrid.json';config.write_text(json.dumps({'inputDir':str(native),'outputDir':str(fields),'domain':domain}))
        regrid_seconds+=execute_stage(['node',str(ROOT/'scripts/regrid.mjs'),str(config)],monitor,
            log_dir/f'F{lead:03}-{domain}-regrid.log',STAGE_BUDGETS['regrid'],'重网格',deadline=deadline)
        shutil.rmtree(native);sources.append({**source,**metrics});monitor.sample();monitor.check()
    return sources,source_fields,extract_seconds,regrid_seconds

def parse_leads(value):
    if value=='all':return list(range(0,97,3))
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
    parser.add_argument('--shard',default=None,help='分片序号/总数，例如 2/4')
    parser.add_argument('--byte-budget',type=int,default=None,help='本进程允许写入的字节上限')
    parser.add_argument('--skip-prepare',action='store_true',help='不清理旧批次，仅追加本分片时次')
    parser.add_argument('--prepare-only',action='store_true',help='只准备批次（取租约、清旧批、写进度）后退出')
    args=parser.parse_args()
    if not 0<=args.maxzoom<=5:parser.error('缩放层级只允许0至5')
    if not 1<=args.product_workers<=4:parser.error('产品并行数只允许1至4')
    shard=None
    if args.shard:
        try:index_text,count_text=args.shard.split('/');shard=(int(index_text),int(count_text))
        except ValueError:parser.error('分片参数格式为 序号/总数')
        try:expected=shard_leads(*shard)
        except SafetyStop as error:parser.error(str(error))
        if not expected:parser.error('该分片没有时次')
    if args.byte_budget is not None and args.byte_budget<=0:parser.error('字节预算必须为正数')
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
    if shard and args.leads=='all':leads=expected
    definition=product_definition(model,args.profile)
    if args.model:
        complete=args.profile=='full' and leads==FULL_LEADS
        partial=bool(shard) and args.profile=='full' and leads==expected
        if args.mode=='r2' and not ((args.profile=='500hpa' and leads==[0]) or complete or partial):
            parser.error('R2发布支持500hPa/F000首图、完整产品全时次或单个分片')
        scope='full' if complete or partial else 'preview'
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
    # 分片共用同一个起报与指纹，否则 4 个分片会各算出一个 run_id。
    processing={'model':model,'profile':args.profile,'maxzoom':args.maxzoom,'leads':FULL_LEADS if shard else [x['lead'] for x in plan],
        'products':definition['products'],'line_sigma':2,'rain_trace_kernel':[1,4,6,4,1],'rain_display_subdivision':2,'cape_sigma':.5,'other_fill_sigma':2,
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
    monitor=DiskMonitor(work);publisher=None;lease=None;heartbeat=None;started=time.monotonic()
    try:
        with monitor:
            if args.mode!='discard':
                if args.mode=='local':
                    store=LocalStore(args.output,prefix=prefix)
                elif args.model:
                    store=model_store(model,prefix)
                else:
                    store=R2Store(args.bucket)
                lease=acquire_lease(store,shard)
                if not isinstance(lease,LocalStore):
                    heartbeat=LeaseHeartbeat(store,lease,key=lease_key(shard)).start()
                    # 取消时先走 finally 释放租约，而不是留下长时间的空锁。
                    def _terminate(signum,frame):
                        raise SafetyStop('收到终止信号，提前收尾')
                    signal.signal(signal.SIGTERM,_terminate)
                publisher=Publisher(store)
                # 分片模式下每个进程只按自己的额度写入，总和由 shard_budget 保证不越红线。
                if args.byte_budget is not None:publisher.budget=args.byte_budget
                elif shard:publisher.budget=shard_budget(shard[1],retained_bytes(publisher,run_id))
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
                if args.skip_prepare:
                    # 旧批次只由 0 号分片清理；其余分片先等它写出准备标记，绝不删除他人成果。
                    wait_for_prepare(publisher.store,prefix,run_id)
                    publisher.target=run_id
                else:
                    if isinstance(lease,str) and foreign_leases(publisher.store,lease):
                        raise SafetyStop('其他任务仍在写入，暂不清理旧批次')
                    publisher.prepare(run_id)
                progress=prefix+f'runs/{run_id}/progress.json' if not shard else prefix+f'runs/{run_id}/progress-p{shard[0]}.json'
                publisher.put_json(progress,{'run':report['run'],'processing_sha256':fingerprint,'processor_commit':os.environ.get('PROCESSOR_COMMIT'),'status':'incomplete',
                    'shard':args.shard,'leads':[x['lead'] for x in plan]})
                if args.prepare_only:
                    step_summary(['### 批次准备完成','',f"- 模式：{model}｜起报：{report['run']}",f"- run_id：{run_id}",f"- 时次：{len(plan)}"])
                    print(json.dumps({'status':'prepared','run_id':run_id,'leads':len(plan),'run':report['run']}));return
            report['status']='running';failures=[];report['failures']=failures;save()
            for item in plan:
                if time.monotonic()-started>5*3600:raise SafetyStop('整批任务接近时间上限')
                if heartbeat is not None and heartbeat.lost:raise SafetyStop('写入租约已被其他任务接管，本进程停止上传')
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
                try:
                    deadline=time_start+TIMESTEP_BUDGET
                    sources,source_fields,extraction_seconds,regrid_seconds=prepare_fields(item,definition,current,monitor,session,report_path.parent,deadline=deadline)
                    job={'model':model,'fieldsDir':str(current/'fields'),'scratchDir':str(scratch),'outputDir':str(outputs),
                        'profile':args.profile,'lead':lead,'maxzoom':args.maxzoom,'python':sys.executable,'productWorkers':args.product_workers}
                    config=current/'job.json';config.write_text(json.dumps(job))
                    render_script='render-pool.mjs' if args.product_workers>1 else 'render.mjs'
                    render_seconds=execute_stage(['node','--max-old-space-size=4096',str(ROOT/'scripts'/render_script),str(config)],monitor,
                        report_path.parent/f'F{lead:03}.log',STAGE_BUDGETS['render'],'渲染',deadline=deadline)
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
                except (StageTimeout,StageFailure) as error:
                    # 单个时次两次都没成：跳过它继续，绝不把整批和已完成的时次一起丢掉。
                    failures.append({'lead':lead,'reason':str(error)})
                    if len(failures)>MAX_SKIPS:raise SafetyStop(f'连续时次失败过多（{len(failures)} 个），停止本片')
                    report['failures']=failures;report['current_lead']=None;save()
                    print(json.dumps({'lead':lead,'status':'skipped','reason':str(error)},ensure_ascii=False),flush=True)
                    shutil.rmtree(current,ignore_errors=True)
                    continue
            if len(report['times'])+len(failures)!=len(plan):raise ValueError('整批时次未全部完成')
            complete=not failures
            if not complete:
                # 有跳过的时次：本轮不写完成索引，插件按已发布的时次数显示，下一轮从这里续做。
                print(json.dumps({'status':'incomplete','run_id':run_id,'completed':len(report['times']),
                    'skipped':[row['lead'] for row in failures],'total':len(plan)},ensure_ascii=False),flush=True)
            if publisher:
                times=list(report['times']);expected=[t['lead'] for t in plan]
                if shard:
                    covered={row['lead'] for row in publisher.available['times']}
                    if covered!=set(FULL_LEADS):
                        # 其他分片还没写完，本轮先不写完整批次索引，等最后一片收尾。
                        print(json.dumps({'shard':args.shard,'status':'waiting_other_shards','covered':len(covered),'total':len(FULL_LEADS)}),flush=True)
                        times=None;expected=FULL_LEADS
                    else:
                        times=[{'lead':row['lead'],'valid':row['valid'],'product_bytes':row['product_bytes']} for row in publisher.available['times']]
                        expected=FULL_LEADS
                if times and not complete:
                    # 有跳过的时次：本轮不写完成索引，插件按已发布的时次数显示，下一轮从这里续做。
                    print(json.dumps({'status':'incomplete','run_id':run_id,'completed':len(report['times']),
                        'skipped':[row['lead'] for row in failures],'total':len(plan)}),flush=True)
                    times=None
                if times:
                    manifest={'run_id':run_id,'run':report['run'],'processing_sha256':fingerprint,'status':'complete',
                        'profile':args.profile,'times':[{'lead':t['lead'],'valid':t['valid'],'product_bytes':t['product_bytes']} for t in times],
                        'product_bytes':sum(t['product_bytes'] for t in times),'model':model,'attribution':f"Open-Meteo / {'ECMWF' if model=='IFS' else 'NOAA'}, CC BY 4.0"}
                    publisher.finish_run(manifest,expected)
                report['store']={'observed_peak_bytes':publisher.peak_bytes,'final_tracked_bytes':publisher.current_bytes,
                    'batch_count':len(publisher.batches),'application_requests':publisher.store.requests,
                    'note':'SDK 自动重试次数不在 application_requests 中；A 类预算按每次调用最多两次尝试预留。'}
            report['status']='complete' if complete else 'incomplete'
            skipped=('｜跳过：'+','.join(f"F{row['lead']:03d}" for row in failures)) if failures else ''
            step_summary(['### 制图完成' if complete else '### 制图部分完成','',f"- 模式：{model}｜起报：{report['run']}",
                f"- 时次：{len(report['times'])}/{len(plan)}｜成品字节：{sum(t['product_bytes'] for t in report['times'])}{skipped}"])
    except Exception as error:
        report['status']='safety_stop' if isinstance(error,SafetyStop) else 'failed'
        # SDK 错误可能包含账户信息；公开日志只保留错误类型，详细错误不打印凭据。
        detail=public_error(error,args.mode);STOP_DETAIL['detail']=detail
        report['error']=detail
        step_summary(['### 制图停止','',f"- 原因：{detail}",f"- 状态：{report['status']}",f"- 已完成时次：{len(report.get('times',[]))}"])
        raise
    finally:
        release_error=None
        if heartbeat is not None:heartbeat.stop()
        if lease is not None:
            try:release_lease(store,lease,shard)
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

def wait_for_prepare(store,prefix,run_id,timeout=180,interval=5):
    """分片等到准备 job 写好标记再上传；超时即停止，绝不自作主张清理旧批。"""
    key=prefix+f'runs/{run_id}/progress-p0.json'
    deadline=time.monotonic()+timeout
    while store.read_json(key) is None:
        if time.monotonic()>deadline:raise SafetyStop('批次准备未完成，分片拒绝开始上传')
        time.sleep(interval)
    return key

LEASE_SECONDS=1800
LEASE_RENEW_SECONDS=600

LOCK_NAME='writer-lock.json'
LOCK_SHARDS=16

def lease_key(shard=None):
    """每片一把写入租约：同一轮的多片可以并行，不同轮之间仍然互斥。"""
    return PREFIX+(f'writer-lock-p{shard[0]}.json' if shard else LOCK_NAME)

def lease_keys():
    """所有可能存在的写入租约键，供「是否有任务正在写」的判定使用。"""
    return [PREFIX+LOCK_NAME]+[PREFIX+f'writer-lock-p{index}.json' for index in range(LOCK_SHARDS)]

def active_leases(store,now=None):
    """所有仍在有效期内的写入租约（键, 内容）。"""
    moment=time.time() if now is None else now
    return [(key,lock) for key in lease_keys()
        if (lock:=store.read_json(key)) and lock.get('expires',0)>moment]

def active_lease(store,now=None):
    """返回仍在有效期内的写入租约（键, 内容）；全部过期则返回 None。"""
    leases=active_leases(store,now)
    return leases[0] if leases else None

def foreign_leases(store,token,now=None):
    """排除自己持有的租约后，还在有效期内的其他写入者。"""
    return [lock for _,lock in active_leases(store,now) if lock.get('token')!=token]

class LeaseHeartbeat:
    """把租约改成短 TTL + 心跳：进程被取消后最多 LEASE_SECONDS 就自动失效，不会再空锁七小时。"""
    def __init__(self,store,token,prefix=PREFIX,seconds=LEASE_SECONDS,renew=LEASE_RENEW_SECONDS,key=None):
        self.store=store;self.token=token;self.prefix=prefix;self.seconds=seconds;self.renew=renew
        self.key=key or prefix+LOCK_NAME
        self.lost=False;self.beats=0;self._stop=threading.Event();self._thread=None
    def beat(self):
        lock=self.store.read_json(self.key)
        if not lock or lock.get('token')!=self.token:
            self.lost=True;return False
        self.store.put(self.key,encoded({'token':self.token,'expires':time.time()+self.seconds}))
        self.beats+=1;return True
    def _loop(self):
        while not self._stop.wait(self.renew):
            if not self.beat():return
    def start(self):
        self._thread=threading.Thread(target=self._loop,daemon=True,name='lease-heartbeat');self._thread.start();return self
    def stop(self):
        self._stop.set()

def acquire_lease(store,shard=None):
    """排他租约：TTL 由心跳续期（默认 30 分钟，每 10 分钟一次），进程被取消也不会长时间占锁。"""
    import uuid
    token=uuid.uuid4().hex;payload={'token':token,'expires':time.time()+LEASE_SECONDS}
    key=lease_key(shard)
    if isinstance(store,LocalStore):
        import fcntl
        stream=(store.root/(f'.writer-p{shard[0]}.lock' if shard else '.writer.lock')).open('w')
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

def release_lease(store,lease,shard=None):
    if isinstance(store,LocalStore):lease.close();return
    key=lease_key(shard)
    current=store.read_json(key)
    if not current or current.get('token')!=lease or current.get('expires',0)<=time.time():
        raise SafetyStop('写入租约归属或有效期异常')
    store.delete([key])

if __name__=='__main__':
    try:main()
    except Exception as error:
        # 详细公开错误已由报告脱敏；不把 SDK 响应和环境变量直接输出。
        detail=STOP_DETAIL.get('detail','')
        print(f'任务停止：{type(error).__name__}'+(f'：{detail}' if detail else ''),file=sys.stderr);sys.exit(2)
