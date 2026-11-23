"""按时次预留空间；上传循环仅做整数计数，不逐文件远端扫描。"""
from pathlib import Path,PurePosixPath
from datetime import date,timedelta
import base64,hashlib,json,os,random,re,shutil,time
from uuid import uuid4

PREFIX='ifs-test-v1/'
RUN_ID=re.compile(r'\d{10}-[a-f0-9]{12}')
OWNER={'protocol':'open-weather-map-batch-ifs-v1','prefix':PREFIX}
CEILING_BYTES=9_600_000_000
HEADROOM_BYTES=256_000_000
METADATA_RESERVATION=2_000_000
BUDGET_WINDOW_DAYS=35
CLASS_A_ATTEMPTS=2
CONDITIONAL_WRITE_ATTEMPTS=6
RECENT_RESERVATIONS=16
UNCONDITIONAL=object()

class SafetyStop(RuntimeError):
    """保险丝触发：不得降级为成功、继续上传或自动扩大限制。"""

class WriteConflict(Exception):
    """对象在读取后被其他写入者更新。"""

class WriteThrottled(Exception):
    """R2 对控制清单的条件写入限流。"""

class WriteUncertain(Exception):
    """R2 可能已完成条件写入，但没有确认结果。"""

TRANSIENT_CODES=('InternalError','ServiceUnavailable','SlowDown','RequestTimeout','Throttling','RequestTimeTooSkewed','500','502','503','504')

def transient_error(error):
    """存储服务的临时故障：5xx、限流，或连接/读取超时（这一类没有 HTTP 响应，按临时处理）。"""
    if type(error).__module__.split('.')[0] not in ('botocore','boto3'):return False
    response=getattr(error,'response',None)
    if response is None:return True
    status=response.get('ResponseMetadata',{}).get('HTTPStatusCode',0) or 0
    return status>=500 or response.get('Error',{}).get('Code','') in TRANSIENT_CODES

def retained_bytes(publisher,run_id):
    """本轮之外仍在桶里的字节（旧完整批）：分片预算必须先扣掉它，才能保证总量不越线。"""
    runs=publisher.prefix+'runs/';marker=runs+run_id+'/'
    # 只统计旧批次对象；owner/available/latest 等元数据已由 METADATA_RESERVATION 覆盖。
    return sum(row['bytes'] for key,row in publisher.objects.items() if key.startswith(runs) and not key.startswith(marker))

def shard_budget(shard_count,retained_bytes=0):
    """每个分片进程允许写入的字节上限：所有分片加起来也越不过容量红线。

    该额度由本进程自己计数执行，不依赖远端快照，因此与并发时序无关。
    """
    if not 1<=shard_count<=16:raise SafetyStop('分片数量只允许1至16')
    if retained_bytes<0:raise SafetyStop('保留字节数无效')
    metadata=METADATA_RESERVATION*(shard_count+2)
    available=CEILING_BYTES-HEADROOM_BYTES-retained_bytes-metadata
    if available<=0:raise SafetyStop('容量余量不足，禁止分片发布')
    return available//shard_count

def reserved_class_a(products,times,listed_objects):
    max_objects=listed_objects+(products+1)*times+20
    max_pages=max(1,(max_objects+999)//1000)
    # 每时次更新一次可用目录，另给操作账本和批次完成各一次条件更新。
    conditional=times+2
    ordinary=products*times+times+28+(times+4)*max_pages
    return ordinary*CLASS_A_ATTEMPTS+conditional*CONDITIONAL_WRITE_ATTEMPTS

def encoded(value):
    return (json.dumps(value,ensure_ascii=False,separators=(',',':'))+'\n').encode()

def digests(path):
    sha,md5=hashlib.sha256(),hashlib.md5()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda:stream.read(1024*1024),b''):
            sha.update(chunk);md5.update(chunk)
    return sha.hexdigest(),md5.hexdigest()

def checked_key(key,prefix=PREFIX):
    # 同一桶的全部发布范围共用租约和滚动账本，沿用已有预算。
    # 写入租约按分片各一把，未分片时仍用 writer-lock.json；它们都在共享根前缀下。
    shared=re.fullmatch(re.escape(PREFIX)+r'writer-lock(-p\d{1,2})?\.json',key) or re.fullmatch(re.escape(PREFIX)+r'budgets/(rolling-operations|[0-9]{4}-[0-9]{2})\.json',key)
    if (not key.startswith(prefix) and not shared) or '..' in PurePosixPath(key).parts or '\\' in key:
        raise SafetyStop('禁止操作测试前缀以外的对象')
    return key

def run_ids(objects,prefix=PREFIX):
    result=set()
    for key in objects:
        if key.startswith(prefix+'runs/'):
            rest=key[len(prefix+'runs/'):];name=rest.split('/')[0]
            if not RUN_ID.fullmatch(name):
                raise SafetyStop('测试区存在无法识别的批次，须人工核查')
            result.add(name)
    return result

class LocalStore:
    def __init__(self,root,prefix=PREFIX):
        self.prefix=prefix
        self.root=Path(root).resolve();self.root.mkdir(parents=True,exist_ok=True)
        self.hash_cache={};self.requests={'list':0,'get':0,'put':0,'delete':0}

    def _path(self,key):
        return self.root/checked_key(key,self.prefix)

    def list(self):
        self.requests['list']+=1;result={}
        for path in self.root.rglob('*'):
            if path.is_symlink():
                raise SafetyStop('测试存储中不能出现符号链接')
            if not path.is_file():continue
            stat=path.stat();identity=(str(path),stat.st_size,stat.st_mtime_ns)
            if identity not in self.hash_cache:self.hash_cache[identity]=digests(path)[1]
            result[path.relative_to(self.root).as_posix()]={'bytes':stat.st_size,'etag':self.hash_cache[identity]}
        return result

    def read_json(self,key):
        return self.read_json_version(key)[0]

    def read_json_version(self,key):
        self.requests['get']+=1;path=self._path(key)
        try:
            with path.open('rb') as stream:data=stream.read(2_000_001)
        except FileNotFoundError:return None,None
        if len(data)>2_000_000:raise SafetyStop('控制清单过大，禁止继续处理')
        return json.loads(data),hashlib.md5(data).hexdigest()

    def put(self,key,payload,immutable=False,expected=UNCONDITIONAL):
        import fcntl
        self.requests['put']+=1;path=self._path(key);path.parent.mkdir(parents=True,exist_ok=True)
        data=payload.read_bytes() if isinstance(payload,Path) else payload
        etag=hashlib.md5(data).hexdigest()
        lock=os.open(self.root,os.O_RDONLY)
        try:
            fcntl.flock(lock,fcntl.LOCK_EX)
            if expected is not UNCONDITIONAL:
                current=digests(path)[1] if path.exists() else None
                if current!=expected:raise WriteConflict(key)
            if immutable and path.exists():
                if path.read_bytes()!=data:raise SafetyStop('不可覆盖已存在的不同成品')
                return etag
            temp=path.with_name(path.name+'.uploading');temp.write_bytes(data);temp.replace(path)
            return etag
        finally:os.close(lock)

    def delete(self,keys):
        for key in keys:
            self.requests['delete']+=1;self._path(key).unlink(missing_ok=True)

class R2Store:
    def __init__(self,bucket,max_list_pages=100,prefix=PREFIX,credentials=None):
        self.prefix=prefix
        credentials=os.environ if credentials is None else credentials
        import boto3
        from botocore.config import Config
        self.bucket=bucket;self.max_list_pages=max_list_pages;self.requests={'list':0,'get':0,'put':0,'delete':0}
        account=credentials.get('R2_ACCOUNT_ID','')
        if not re.fullmatch('[a-f0-9]{32}',account) or not bucket:
            raise SafetyStop('缺少有效的 R2 账户或桶配置')
        if not credentials.get('AWS_ACCESS_KEY_ID') or not credentials.get('AWS_SECRET_ACCESS_KEY'):
            raise SafetyStop('缺少受限的 R2 读写凭据；不使用匿名或公开访问替代')
        def client(attempts):
            return boto3.client('s3',endpoint_url=f'https://{account}.r2.cloudflarestorage.com',
                aws_access_key_id=credentials['AWS_ACCESS_KEY_ID'],aws_secret_access_key=credentials['AWS_SECRET_ACCESS_KEY'],
                region_name='auto',config=Config(retries={'total_max_attempts':attempts,'mode':'standard'},
                connect_timeout=15,read_timeout=90,request_checksum_calculation='when_required',
                response_checksum_validation='when_required'))
        self.client=client(CLASS_A_ATTEMPTS)
        self.read_client=client(5)
        # 外层每轮发一次 PUT，冲突和限流共用同一预算。
        self.conditional_client=client(1)

    def list(self):
        result={};token=None;seen=set();pages=0
        while True:
            pages+=1
            if pages>self.max_list_pages:raise SafetyStop('对象清单分页过多，停止以避免无限读取')
            args={'Bucket':self.bucket}
            if token:args['ContinuationToken']=token
            self.requests['list']+=1;page=self.client.list_objects_v2(**args)
            for row in page.get('Contents',[]):
                result[row['Key']]={'bytes':row['Size'],'etag':row.get('ETag','').strip('"')}
            if not page.get('IsTruncated'):return result
            token=page.get('NextContinuationToken')
            if not token:raise SafetyStop('对象列表未完整返回，禁止继续上传')
            if token in seen:raise SafetyStop('对象列表分页令牌重复，停止循环读取')
            seen.add(token)

    def read_json(self,key):
        return self.read_json_version(key)[0]

    def read_json_version(self,key):
        from botocore.exceptions import ClientError
        self.requests['get']+=1
        try:
            response=self.read_client.get_object(Bucket=self.bucket,Key=checked_key(key,self.prefix))
        except ClientError as error:
            if error.response['Error']['Code'] in ('NoSuchKey','404'):return None,None
            raise
        with response['Body'] as body:
            if response.get('ContentLength',2_000_001)>2_000_000:raise SafetyStop('控制清单过大，禁止继续处理')
            data=body.read(2_000_001)
            if len(data)>2_000_000:raise SafetyStop('控制清单长度异常')
            return json.loads(data),response.get('ETag')

    def put(self,key,payload,immutable=False,expected=UNCONDITIONAL):
        from botocore.exceptions import ClientError,ConnectionError,HTTPClientError
        key=checked_key(key,self.prefix);data=payload.read_bytes() if isinstance(payload,Path) else payload
        # 单成品最多 512 MB；不用分段上传，避免未完成分段累积和额外 A 操作。
        if len(data)>512_000_000:raise SafetyStop('单成品过大，须重新评估')
        md5=hashlib.md5(data).digest();self.requests['put']+=1
        args={'Bucket':self.bucket,'Key':key,'Body':data,'ContentLength':len(data),
              'ContentMD5':base64.b64encode(md5).decode(),'ContentType':'application/json' if key.endswith('.json') else 'application/octet-stream',
              'StorageClass':'STANDARD'}
        if immutable or expected is None:args['IfNoneMatch']='*'
        elif expected is not UNCONDITIONAL:args['IfMatch']=expected
        conditional=expected is not UNCONDITIONAL
        client=self.conditional_client if conditional else self.client
        try:response=client.put_object(**args)
        except ClientError as error:
            code=error.response['Error']['Code']
            status=error.response.get('ResponseMetadata',{}).get('HTTPStatusCode')
            if conditional:
                if status==429 or code in ('429','SlowDown','Throttling','ThrottlingException','TooManyRequests','TooManyRequestsException'):
                    raise WriteThrottled(key) from error
                if status in (409,412) or code in ('PreconditionFailed','412','ConditionalRequestConflict','409'):
                    raise WriteConflict(key) from error
                if transient_error(error):raise WriteUncertain(key) from error
            if status==412 or code in ('PreconditionFailed','412'):
                if immutable:
                    self.requests['get']+=1
                    existing=self.read_client.head_object(Bucket=self.bucket,Key=key)
                    if existing.get('ContentLength')==len(data) and existing.get('ETag','').strip('"')==md5.hex():
                        return md5.hex()
                raise SafetyStop('对象已由其他任务写入，禁止覆盖') from error
            raise
        except (ConnectionError,HTTPClientError) as error:
            if conditional:raise WriteUncertain(key) from error
            raise
        etag=response.get('ETag','').strip('"')
        if etag!=md5.hex():raise SafetyStop('上传返回的 ETag 与本地 MD5 不一致')
        return etag

    def delete(self,keys):
        # 每批 200 个：单请求越轻越不容易撞读取超时，失败按临时故障重试。
        for start in range(0,len(keys),200):
            items=[{'Key':checked_key(key,self.prefix)} for key in keys[start:start+200]]
            for attempt in range(3):
                self.requests['delete']+=1
                try:
                    response=self.client.delete_objects(Bucket=self.bucket,Delete={'Objects':items,'Quiet':True})
                except Exception as error:
                    if not transient_error(error) or attempt==2:raise
                    time.sleep(2*(attempt+1));continue
                if response.get('Errors'):raise SafetyStop('旧测试批次清理不完整，禁止上传新批次')
                break

class Publisher:
    def __init__(self,store,ceiling=CEILING_BYTES,reserve=HEADROOM_BYTES,budget=None):
        if not 0<ceiling<=CEILING_BYTES or reserve<0:
            raise SafetyStop('不能扩大保险丝上限')
        if budget is not None and not 0<budget<=CEILING_BYTES:
            raise SafetyStop('分片字节预算无效')
        self.prefix=store.prefix
        self.store=store;self.ceiling=ceiling;self.reserve=reserve
        self.budget=budget;self.written=0
        self.refresh();self.peak_bytes=self.current_bytes
        self.latest=store.read_json(self.prefix+'latest.json');self.remaining=0;self.target=None
        self.available=store.read_json(self.prefix+'available.json')
        self._check()
        expected_owner=OWNER if self.prefix==PREFIX else {'protocol':'weather-tiles-v1','prefix':self.prefix}
        owner=store.read_json(self.prefix+'owner.json')
        if owner is None:
            if any(k.startswith(self.prefix) and k!=self.prefix+'writer-lock.json' for k in self.objects):
                raise SafetyStop('测试前缀已有未知资料，禁止接管或删除')
            self.remaining=METADATA_RESERVATION;self.put_json(self.prefix+'owner.json',expected_owner)
        elif owner!=expected_owner:raise SafetyStop('测试前缀归属不符')
        self.remaining=METADATA_RESERVATION

    def _check(self,additional=0,new_run=None):
        batches=self.batches
        if len(batches)>2 or new_run and len(batches|{new_run})>2:
            raise SafetyStop('批次数保险丝：不能存在超过两批')
        total=self.current_bytes
        if total+additional+self.reserve>self.ceiling:
            raise SafetyStop('存储保险丝：当前占用、待上传及余量将超过安全上限')
        return total

    def refresh(self):
        self.objects=self.store.list()
        self.current_bytes=sum(x['bytes'] for x in self.objects.values())
        self.batches=run_ids(self.objects,self.prefix)

    def prepare(self,run_id):
        if not RUN_ID.fullmatch(run_id):raise SafetyStop('批次名称无效')
        self.target=run_id;self.refresh();self._check()
        keep={run_id}
        if self.latest:
            latest_id=self.latest.get('run_id','')
            if not RUN_ID.fullmatch(latest_id) or self.latest.get('status')!='complete':
                raise SafetyStop('最新完整批次索引异常，禁止清理旧资料')
            actual=self.store.read_json(self.prefix+f'runs/{latest_id}/manifest.json')
            if actual!=self.latest:raise SafetyStop('最新完整批次清单缺失或不一致')
            keep.add(latest_id)
        elif any(self.prefix+f'runs/{r}/manifest.json' in self.objects and
                 not (r==run_id and (self.available or {}).get('run_id')==r) for r in self.batches):
            raise SafetyStop('存在完整批次但最新索引缺失，须先恢复索引')
        self.remaining=METADATA_RESERVATION
        if not self.available or self.available.get('run_id')!=run_id:
            # 在清理前撤下旧的未完成入口，仍提供最近完整批次回退。
            self.available={'run_id':run_id,'complete':False,'times':[],
                'fallback_run_id':self.latest['run_id'] if self.latest else None}
            self.put_json(self.prefix+'available.json',self.available)
        # 必须先检查是否已超两批；不通过自动删除掩盖保险丝触发。
        self.remove_other_runs(keep)
        self._check(new_run=run_id)
        self.remaining=METADATA_RESERVATION

    def remove_other_runs(self,keep,defer=False):
        """回收旧批。defer=True 时遇到存储临时故障只记一笔、不判红：此时新批已发布，资料是完整的。"""
        folders=tuple(self.prefix+'runs/'+r+'/' for r in self.batches-keep)
        if not folders:return True
        keys=[k for k in self.objects if k.startswith(folders)]
        try:
            self.store.delete(keys);self.refresh()
        except Exception as error:
            if not defer or not transient_error(error):raise
            print(json.dumps({'status':'cleanup_deferred','pending':sorted(self.batches-keep),
                'reason':type(error).__name__},ensure_ascii=False),flush=True)
            return False
        if self.batches-keep:
            if defer:
                print(json.dumps({'status':'cleanup_deferred','pending':sorted(self.batches-keep),
                    'reason':'未清理完'},ensure_ascii=False),flush=True)
                return False
            raise SafetyStop('旧起报未清理完毕，须重试')
        return True

    def reserve_time(self,files):
        # 每个时次只进行一次完整远端核对，后续每文件仅扣减本地预留额度。
        self.refresh();self._check(new_run=self.target)
        required=METADATA_RESERVATION
        for key,path in files:
            checked_key(key,self.prefix);required+=max(0,Path(path).stat().st_size-self.objects.get(key,{}).get('bytes',0))
        self._check(additional=required,new_run=self.target)
        self.remaining=required

    def put(self,key,payload,immutable=False,expected=UNCONDITIONAL):
        checked_key(key,self.prefix);size=payload.stat().st_size if isinstance(payload,Path) else len(payload)
        previous=self.objects.get(key,{}).get('bytes',0);extra=max(0,size-previous)
        if extra>self.remaining:raise SafetyStop('上传量超过本时次预留空间')
        # 分片额度只看本进程累计写入，因此并发下也不会越过分片总和。
        if self.budget is not None and self.written+extra>self.budget:
            raise SafetyStop('分片字节预算：本进程写入将超过本片额度')
        # 不再查询远端。此检查只有整数运算和少量批次名集合操作。
        new_run=self.target if self.target and key.startswith(self.prefix+f'runs/{self.target}/') else None
        self._check(additional=extra,new_run=new_run)
        if expected is UNCONDITIONAL:
            etag=self.store.put(key,payload,immutable=immutable)
        else:
            etag=self.store.put(key,payload,expected=expected)
        self.remaining-=extra;self.written+=extra
        self.objects[key]={'bytes':size,'etag':etag};self.current_bytes+=size-previous
        if key.startswith(self.prefix+'runs/'):self.batches.add(key[len(self.prefix+'runs/'):].split('/')[0])
        self.peak_bytes=max(self.peak_bytes,self._check())
        return etag

    def put_json(self,key,value):
        return self.put(key,encoded(value))

    def update_json(self,key,update,*,guard_replay=False):
        backed_off=0;uncertain_etag=UNCONDITIONAL;uncertain_error=None
        for attempt in range(CONDITIONAL_WRITE_ATTEMPTS+1):
            current,etag=self.store.read_json_version(key)
            if current is not None and not etag:raise SafetyStop('控制清单缺少对象版本')
            value=update(current)
            if value==current:return current
            if guard_replay and uncertain_etag is not UNCONDITIONAL and etag!=uncertain_etag:
                raise SafetyStop('预算账本已更新，无法确认上一笔预留，停止以避免重复计入') from uncertain_error
            if attempt==CONDITIONAL_WRITE_ATTEMPTS:
                raise SafetyStop('R2 条件写入结果未确认，暂缓写入控制清单') from uncertain_error
            try:
                self.put(key,encoded(value),expected=etag)
                return value
            except (WriteConflict,WriteThrottled,WriteUncertain) as error:
                if isinstance(error,WriteUncertain):
                    uncertain_etag=etag;uncertain_error=error
                if attempt+1==CONDITIONAL_WRITE_ATTEMPTS:
                    if uncertain_error is not None:continue  # 最后只回读确认，不再发 PUT。
                    message='R2 持续限流，暂缓写入控制清单' if isinstance(error,WriteThrottled) else '控制清单并发写入未收敛'
                    raise SafetyStop(message) from error
                if isinstance(error,WriteConflict):
                    delay=min(2,.2*2**attempt)
                else:
                    delay=min(16,2**backed_off)
                    backed_off+=1
                time.sleep(delay+random.uniform(0,.2))

    def matches(self,key,size,md5):
        return self.objects.get(key)=={'bytes':size,'etag':md5}

    def publish_time(self,manifest):
        """只有该时次全部成品在存储中核验一致后，才让它出现在可用目录。"""
        rows=[r for r in manifest['products'] if r['status']=='verified']
        if not rows or not all(self.matches(r['object_key'],r['bytes'],r['md5']) for r in rows):
            raise SafetyStop('时次成品未全部上传并校验，禁止提前开放')
        key=self.prefix+f"runs/{self.target}/F{manifest['lead']:03}/manifest.json"
        if key not in self.objects:raise SafetyStop('时次清单尚未提交，禁止提前开放')
        item={'lead':manifest['lead'],'valid':manifest['valid'],'manifest_key':key,
            'product_bytes':manifest['product_bytes'],'verified_products':len(rows)}
        def merge(remote):
            if not remote or remote.get('run_id')!=self.target:
                raise SafetyStop('当前可用时次目录已切换起报')
            if remote.get('complete'):return remote
            times={row['lead']:row for row in remote.get('times',[])};times[item['lead']]=item
            return {**remote,'model':manifest.get('model'),'run':manifest.get('run'),'processing_sha256':manifest.get('processing_sha256'),
                'profile':manifest.get('profile'),'times':sorted(times.values(),key=lambda x:x['lead'])}
        self.available=self.update_json(self.prefix+'available.json',merge)

    def finish_run(self,manifest,expected_leads):
        if manifest['run_id']!=self.target or self.available['run_id']!=self.target or manifest['status']!='complete':
            raise SafetyStop('完整批次与当前发布目录不符')
        if {r['lead'] for r in self.available['times']}!=set(expected_leads):
            raise SafetyStop('可用时次未覆盖请求范围，不能标记整批完成')
        self.put_json(self.prefix+f'runs/{self.target}/manifest.json',manifest)
        def complete(remote):
            if not remote or remote.get('run_id')!=self.target or {r['lead'] for r in remote['times']}!=set(expected_leads):
                raise SafetyStop('可用时次目录已变更，不能标记整批完成')
            return {**remote,'complete':True,**({'fallback_run_id':None} if self.prefix!=PREFIX else {})}
        self.available=self.update_json(self.prefix+'available.json',complete)
        self.put_json(self.prefix+'latest.json',manifest)
        self.latest=manifest
        # 两份在线目录都已切换，才能回收上一批。旧测试前缀沿用原保留规则。
        if self.prefix!=PREFIX:self.remove_other_runs({self.target},defer=True)

    def reserve_operations(self,day,writes):
        # 覆盖31天账期、批次开始预留至实际上传的跨日延迟，并额外保留余量。
        try:today=date.fromisoformat(day)
        except (TypeError,ValueError):raise SafetyStop('操作预算日期无效')
        if today.isoformat()!=day:raise SafetyStop('操作预算日期必须为YYYY-MM-DD')
        if type(writes) is not int or writes<0:raise SafetyStop('操作预留无效')
        key=PREFIX+'budgets/rolling-operations.json'
        reservation=uuid4().hex
        self.update_json(key,lambda ledger:self._operation_ledger(today,day,writes,reservation,ledger),guard_replay=True)

    def _operation_ledger(self,today,day,writes,reservation,ledger):
        cutoff=today-timedelta(days=BUDGET_WINDOW_DAYS-1)
        recent=[]
        if ledger is None:
            # 旧月账本无法还原逐日用量，首次升级将相关月份的全部预留保守计入今天。
            legacy=0
            months=sorted({(today-timedelta(days=i)).strftime('%Y-%m') for i in range(BUDGET_WINDOW_DAYS)})
            for month in months:
                previous=self.store.read_json(PREFIX+f'budgets/{month}.json')
                if previous is None:continue
                value=previous.get('reserved_class_a') if isinstance(previous,dict) else None
                if type(value) is not int or not 0<=value<=800_000:raise SafetyStop('旧操作预算记录异常')
                legacy+=value
            days={day:legacy} if legacy else {}
        else:
            if not isinstance(ledger,dict) or ledger.get('version')!=1 or ledger.get('window_days')!=BUDGET_WINDOW_DAYS or not isinstance(ledger.get('days'),dict) or len(ledger['days'])>BUDGET_WINDOW_DAYS:
                raise SafetyStop('滚动操作预算记录异常')
            recent=ledger.get('recent_reservations',[])
            if not isinstance(recent,list) or len(recent)>RECENT_RESERVATIONS or any(
                not isinstance(item,str) or not re.fullmatch('[a-f0-9]{32}',item) for item in recent):
                raise SafetyStop('滚动操作预算预留记录异常')
            try:last_day=date.fromisoformat(ledger['as_of'])
            except (KeyError,TypeError,ValueError):raise SafetyStop('滚动操作预算日期异常')
            if last_day.isoformat()!=ledger['as_of']:raise SafetyStop('滚动操作预算日期异常')
            days={};recorded_total=0
            for recorded,value in ledger['days'].items():
                try:stamp=date.fromisoformat(recorded)
                except (TypeError,ValueError):raise SafetyStop('滚动操作预算日期异常')
                if stamp.isoformat()!=recorded or stamp>last_day or stamp<last_day-timedelta(days=BUDGET_WINDOW_DAYS-1) or type(value) is not int or not 0<=value<=800_000:
                    raise SafetyStop('滚动操作预算记录异常')
                recorded_total+=value
                if stamp>=cutoff:days[recorded]=value
            if recorded_total>800_000:raise SafetyStop('滚动操作预算累计值异常')
            if reservation in recent:return ledger
            if today<last_day:raise SafetyStop('操作预算日期倒退，禁止重置预算')
        if sum(days.values())+writes>800_000:
            raise SafetyStop('上传操作保险丝：滚动35天预留A类操作将超过80万次')
        days[day]=days.get(day,0)+writes
        return {'version':1,'window_days':BUDGET_WINDOW_DAYS,'as_of':day,'days':days,
                'recent_reservations':(recent+[reservation])[-RECENT_RESERVATIONS:]}
