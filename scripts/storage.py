"""按时次预留空间；上传循环仅做整数计数，不逐文件远端扫描。"""
from pathlib import Path,PurePosixPath
from datetime import date,timedelta
import base64,hashlib,json,os,re,shutil

PREFIX='ifs-test-v1/'
RUN_ID=re.compile(r'\d{10}-[a-f0-9]{12}')
OWNER={'protocol':'open-weather-map-batch-ifs-v1','prefix':PREFIX}
CEILING_BYTES=9_000_000_000
HEADROOM_BYTES=256_000_000
METADATA_RESERVATION=2_000_000
BUDGET_WINDOW_DAYS=35

class SafetyStop(RuntimeError):
    """保险丝触发：不得降级为成功、继续上传或自动扩大限制。"""

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
    shared=key==PREFIX+'writer-lock.json' or re.fullmatch(re.escape(PREFIX)+r'budgets/(rolling-operations|[0-9]{4}-[0-9]{2})\.json',key)
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
        self.requests['get']+=1;path=self._path(key)
        if not path.exists():return None
        if path.stat().st_size>2_000_000:raise SafetyStop('控制清单过大，禁止继续处理')
        return json.loads(path.read_text())

    def put(self,key,payload,immutable=False):
        self.requests['put']+=1;path=self._path(key);path.parent.mkdir(parents=True,exist_ok=True)
        data=payload.read_bytes() if isinstance(payload,Path) else payload
        if immutable and path.exists():
            if path.read_bytes()!=data:raise SafetyStop('不可覆盖已存在的不同成品')
            return hashlib.md5(data).hexdigest()
        temp=path.with_name(path.name+'.uploading');temp.write_bytes(data);temp.replace(path)
        return hashlib.md5(data).hexdigest()

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
        self.client=boto3.client('s3',endpoint_url=f'https://{account}.r2.cloudflarestorage.com',
            aws_access_key_id=credentials['AWS_ACCESS_KEY_ID'],aws_secret_access_key=credentials['AWS_SECRET_ACCESS_KEY'],
            region_name='auto',config=Config(retries={'total_max_attempts':2,'mode':'standard'},
            connect_timeout=15,read_timeout=90,request_checksum_calculation='when_required',
            response_checksum_validation='when_required'))

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
        from botocore.exceptions import ClientError
        self.requests['get']+=1
        try:
            response=self.client.get_object(Bucket=self.bucket,Key=checked_key(key,self.prefix))
        except ClientError as error:
            if error.response['Error']['Code'] in ('NoSuchKey','404'):return None
            raise
        with response['Body'] as body:
            if response.get('ContentLength',2_000_001)>2_000_000:raise SafetyStop('控制清单过大，禁止继续处理')
            data=body.read(2_000_001)
            if len(data)>2_000_000:raise SafetyStop('控制清单长度异常')
            return json.loads(data)

    def put(self,key,payload,immutable=False):
        from botocore.exceptions import ClientError
        key=checked_key(key,self.prefix);data=payload.read_bytes() if isinstance(payload,Path) else payload
        # 单成品最多 512 MB；不用分段上传，避免未完成分段累积和额外 A 操作。
        if len(data)>512_000_000:raise SafetyStop('单成品过大，须重新评估')
        md5=hashlib.md5(data).digest();self.requests['put']+=1
        args={'Bucket':self.bucket,'Key':key,'Body':data,'ContentLength':len(data),
              'ContentMD5':base64.b64encode(md5).decode(),'ContentType':'application/json' if key.endswith('.json') else 'application/octet-stream',
              'StorageClass':'STANDARD'}
        if immutable:args['IfNoneMatch']='*'
        try:response=self.client.put_object(**args)
        except ClientError as error:
            if error.response['Error']['Code'] in ('PreconditionFailed','412'):
                raise SafetyStop('对象已由其他任务写入，禁止覆盖') from error
            raise
        etag=response.get('ETag','').strip('"')
        if etag!=md5.hex():raise SafetyStop('上传返回的 ETag 与本地 MD5 不一致')
        return etag

    def delete(self,keys):
        for start in range(0,len(keys),1000):
            items=[{'Key':checked_key(key,self.prefix)} for key in keys[start:start+1000]]
            self.requests['delete']+=1
            response=self.client.delete_objects(Bucket=self.bucket,Delete={'Objects':items,'Quiet':True})
            if response.get('Errors'):raise SafetyStop('旧测试批次清理不完整，禁止上传新批次')

class Publisher:
    def __init__(self,store,ceiling=CEILING_BYTES,reserve=HEADROOM_BYTES):
        if not 0<ceiling<=CEILING_BYTES or reserve<0:
            raise SafetyStop('不能扩大保险丝上限')
        self.prefix=store.prefix
        self.store=store;self.ceiling=ceiling;self.reserve=reserve
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

    def remove_other_runs(self,keep):
        folders=tuple(self.prefix+'runs/'+r+'/' for r in self.batches-keep)
        if folders:
            keys=[k for k in self.objects if k.startswith(folders)]
            self.store.delete(keys)
            self.refresh()
            if self.batches-keep:raise SafetyStop('旧起报未清理完毕，须重试')

    def reserve_time(self,files):
        # 每个时次只进行一次完整远端核对，后续每文件仅扣减本地预留额度。
        self.refresh();self._check(new_run=self.target)
        required=METADATA_RESERVATION
        for key,path in files:
            checked_key(key,self.prefix);required+=max(0,Path(path).stat().st_size-self.objects.get(key,{}).get('bytes',0))
        self._check(additional=required,new_run=self.target)
        self.remaining=required

    def put(self,key,payload,immutable=False):
        checked_key(key,self.prefix);size=payload.stat().st_size if isinstance(payload,Path) else len(payload)
        previous=self.objects.get(key,{}).get('bytes',0);extra=max(0,size-previous)
        if extra>self.remaining:raise SafetyStop('上传量超过本时次预留空间')
        # 不再查询远端。此检查只有整数运算和少量批次名集合操作。
        new_run=self.target if self.target and key.startswith(self.prefix+f'runs/{self.target}/') else None
        self._check(additional=extra,new_run=new_run)
        etag=self.store.put(key,payload,immutable=immutable);self.remaining-=extra
        self.objects[key]={'bytes':size,'etag':etag};self.current_bytes+=size-previous
        if key.startswith(self.prefix+'runs/'):self.batches.add(key[len(self.prefix+'runs/'):].split('/')[0])
        self.peak_bytes=max(self.peak_bytes,self._check())
        return etag

    def put_json(self,key,value):
        return self.put(key,encoded(value))

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
        times={row['lead']:row for row in self.available['times']};times[item['lead']]=item
        self.available={**self.available,'model':manifest.get('model'),'run':manifest.get('run'),'processing_sha256':manifest.get('processing_sha256'),
            'profile':manifest.get('profile'),'times':sorted(times.values(),key=lambda x:x['lead'])}
        self.put_json(self.prefix+'available.json',self.available)

    def finish_run(self,manifest,expected_leads):
        if manifest['run_id']!=self.target or self.available['run_id']!=self.target or manifest['status']!='complete':
            raise SafetyStop('完整批次与当前发布目录不符')
        if {r['lead'] for r in self.available['times']}!=set(expected_leads):
            raise SafetyStop('可用时次未覆盖请求范围，不能标记整批完成')
        self.put_json(self.prefix+f'runs/{self.target}/manifest.json',manifest)
        self.available={**self.available,'complete':True}
        if self.prefix!=PREFIX:self.available['fallback_run_id']=None
        self.put_json(self.prefix+'available.json',self.available)
        self.put_json(self.prefix+'latest.json',manifest)
        self.latest=manifest
        # 两份在线目录都已切换，才能回收上一批。旧测试前缀沿用原保留规则。
        if self.prefix!=PREFIX:self.remove_other_runs({self.target})

    def reserve_operations(self,day,writes):
        # 覆盖31天账期、批次开始预留至实际上传的跨日延迟，并额外保留余量。
        try:today=date.fromisoformat(day)
        except (TypeError,ValueError):raise SafetyStop('操作预算日期无效')
        if today.isoformat()!=day:raise SafetyStop('操作预算日期必须为YYYY-MM-DD')
        if type(writes) is not int or writes<0:raise SafetyStop('操作预留无效')
        key=PREFIX+'budgets/rolling-operations.json';ledger=self.store.read_json(key)
        cutoff=today-timedelta(days=BUDGET_WINDOW_DAYS-1)
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
            try:last_day=date.fromisoformat(ledger['as_of'])
            except (KeyError,TypeError,ValueError):raise SafetyStop('滚动操作预算日期异常')
            if last_day.isoformat()!=ledger['as_of']:raise SafetyStop('滚动操作预算日期异常')
            if today<last_day:raise SafetyStop('操作预算日期倒退，禁止重置预算')
            days={};recorded_total=0
            for recorded,value in ledger['days'].items():
                try:stamp=date.fromisoformat(recorded)
                except (TypeError,ValueError):raise SafetyStop('滚动操作预算日期异常')
                if stamp.isoformat()!=recorded or stamp>last_day or stamp<last_day-timedelta(days=BUDGET_WINDOW_DAYS-1) or type(value) is not int or not 0<=value<=800_000:
                    raise SafetyStop('滚动操作预算记录异常')
                recorded_total+=value
                if stamp>=cutoff:days[recorded]=value
            if recorded_total>800_000:raise SafetyStop('滚动操作预算累计值异常')
        if sum(days.values())+writes>800_000:
            raise SafetyStop('上传操作保险丝：滚动35天预留A类操作将超过80万次')
        days[day]=days.get(day,0)+writes
        self.put_json(key,{'version':1,'window_days':BUDGET_WINDOW_DAYS,'as_of':day,'days':days})
