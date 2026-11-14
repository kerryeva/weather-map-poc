"""按时次预留空间；上传循环仅做整数计数，不逐文件远端扫描。"""
from pathlib import Path,PurePosixPath
import base64,hashlib,json,os,re,shutil

PREFIX='ifs-test-v1/'
RUN_ID=re.compile(r'\d{10}-[a-f0-9]{12}')
OWNER={'protocol':'open-weather-map-batch-ifs-v1','prefix':PREFIX}
CEILING_BYTES=8_000_000_000
HEADROOM_BYTES=256_000_000
METADATA_RESERVATION=2_000_000

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

def checked_key(key):
    if not key.startswith(PREFIX) or '..' in PurePosixPath(key).parts or '\\' in key:
        raise SafetyStop('禁止操作测试前缀以外的对象')
    return key

def run_ids(objects):
    result=set()
    for key in objects:
        if key.startswith(PREFIX+'runs/'):
            rest=key[len(PREFIX+'runs/'):];name=rest.split('/')[0]
            if not RUN_ID.fullmatch(name):
                raise SafetyStop('测试区存在无法识别的批次，须人工核查')
            result.add(name)
    return result

class LocalStore:
    def __init__(self,root):
        self.root=Path(root).resolve();self.root.mkdir(parents=True,exist_ok=True)
        self.hash_cache={};self.requests={'list':0,'get':0,'put':0,'delete':0}

    def _path(self,key):
        return self.root/checked_key(key)

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
    def __init__(self,bucket):
        import boto3
        from botocore.config import Config
        self.bucket=bucket;self.requests={'list':0,'get':0,'put':0,'delete':0}
        account=os.environ.get('R2_ACCOUNT_ID','')
        if not re.fullmatch('[a-f0-9]{32}',account) or not bucket:
            raise SafetyStop('缺少有效的 R2 账户或桶配置')
        if not os.environ.get('AWS_ACCESS_KEY_ID') or not os.environ.get('AWS_SECRET_ACCESS_KEY'):
            raise SafetyStop('缺少受限的 R2 读写凭据；不使用匿名或公开访问替代')
        self.client=boto3.client('s3',endpoint_url=f'https://{account}.r2.cloudflarestorage.com',
            region_name='auto',config=Config(retries={'total_max_attempts':2,'mode':'standard'},
            connect_timeout=15,read_timeout=90,request_checksum_calculation='when_required',
            response_checksum_validation='when_required'))

    def list(self):
        result={};token=None
        while True:
            args={'Bucket':self.bucket}
            if token:args['ContinuationToken']=token
            self.requests['list']+=1;page=self.client.list_objects_v2(**args)
            for row in page.get('Contents',[]):
                result[row['Key']]={'bytes':row['Size'],'etag':row.get('ETag','').strip('"')}
            if not page.get('IsTruncated'):return result
            token=page.get('NextContinuationToken')
            if not token:raise SafetyStop('对象列表未完整返回，禁止继续上传')

    def read_json(self,key):
        from botocore.exceptions import ClientError
        self.requests['get']+=1
        try:
            response=self.client.get_object(Bucket=self.bucket,Key=checked_key(key))
        except ClientError as error:
            if error.response['Error']['Code'] in ('NoSuchKey','404'):return None
            raise
        with response['Body'] as body:return json.loads(body.read())

    def put(self,key,payload,immutable=False):
        from botocore.exceptions import ClientError
        key=checked_key(key);data=payload.read_bytes() if isinstance(payload,Path) else payload
        # 单成品最多 512 MB；不用分段上传，避免未完成分段累积和额外 A 操作。
        if len(data)>512_000_000:raise SafetyStop('单成品过大，须重新评估')
        md5=hashlib.md5(data).digest();self.requests['put']+=1
        args={'Bucket':self.bucket,'Key':key,'Body':data,'ContentLength':len(data),
              'ContentMD5':base64.b64encode(md5).decode(),'ContentType':'application/json' if key.endswith('.json') else 'application/octet-stream'}
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
            items=[{'Key':checked_key(key)} for key in keys[start:start+1000]]
            self.requests['delete']+=1
            response=self.client.delete_objects(Bucket=self.bucket,Delete={'Objects':items,'Quiet':True})
            if response.get('Errors'):raise SafetyStop('旧测试批次清理不完整，禁止上传新批次')

class Publisher:
    def __init__(self,store,ceiling=CEILING_BYTES,reserve=HEADROOM_BYTES):
        if not 0<ceiling<=CEILING_BYTES or reserve<0:
            raise SafetyStop('不能扩大保险丝上限')
        self.store=store;self.ceiling=ceiling;self.reserve=reserve
        self.refresh();self.peak_bytes=self.current_bytes
        self.latest=store.read_json(PREFIX+'latest.json');self.remaining=0;self.target=None
        self._check()
        owner=store.read_json(PREFIX+'owner.json')
        if owner is None:
            if any(k.startswith(PREFIX) for k in self.objects):
                raise SafetyStop('测试前缀已有未知资料，禁止接管或删除')
            self.remaining=METADATA_RESERVATION;self.put_json(PREFIX+'owner.json',OWNER)
        elif owner!=OWNER:raise SafetyStop('测试前缀归属不符')

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
        self.batches=run_ids(self.objects)

    def prepare(self,run_id):
        if not RUN_ID.fullmatch(run_id):raise SafetyStop('批次名称无效')
        self.target=run_id;self.refresh();self._check()
        keep={run_id}
        if self.latest:
            latest_id=self.latest.get('run_id','')
            if not RUN_ID.fullmatch(latest_id) or self.latest.get('status')!='complete':
                raise SafetyStop('最新完整批次索引异常，禁止清理旧资料')
            actual=self.store.read_json(PREFIX+f'runs/{latest_id}/manifest.json')
            if actual!=self.latest:raise SafetyStop('最新完整批次清单缺失或不一致')
            keep.add(latest_id)
        elif any(PREFIX+f'runs/{r}/manifest.json' in self.objects for r in self.batches):
            raise SafetyStop('存在完整批次但最新索引缺失，须先恢复索引')
        # 必须先检查是否已超两批；不通过自动删除掩盖保险丝触发。
        old=run_ids(self.objects)-keep
        if old:
            keys=[k for k in self.objects if any(k.startswith(PREFIX+'runs/'+r+'/') for r in old)]
            self.store.delete(keys)
            self.refresh()
        self._check(new_run=run_id)
        self.remaining=METADATA_RESERVATION

    def reserve_time(self,files):
        # 每个时次只进行一次完整远端核对，后续每文件仅扣减本地预留额度。
        self.refresh();self._check(new_run=self.target)
        required=METADATA_RESERVATION
        for key,path in files:
            checked_key(key);required+=max(0,Path(path).stat().st_size-self.objects.get(key,{}).get('bytes',0))
        self._check(additional=required,new_run=self.target)
        self.remaining=required

    def put(self,key,payload,immutable=False):
        checked_key(key);size=payload.stat().st_size if isinstance(payload,Path) else len(payload)
        previous=self.objects.get(key,{}).get('bytes',0);extra=max(0,size-previous)
        if extra>self.remaining:raise SafetyStop('上传量超过本时次预留空间')
        # 不再查询远端。此检查只有整数运算和少量批次名集合操作。
        self._check(additional=extra,new_run=self.target)
        etag=self.store.put(key,payload,immutable=immutable);self.remaining-=extra
        self.objects[key]={'bytes':size,'etag':etag};self.current_bytes+=size-previous
        if key.startswith(PREFIX+'runs/'):self.batches.add(key[len(PREFIX+'runs/'):].split('/')[0])
        self.peak_bytes=max(self.peak_bytes,self._check())
        return etag

    def put_json(self,key,value):
        return self.put(key,encoded(value))

    def matches(self,key,size,md5):
        return self.objects.get(key)=={'bytes':size,'etag':md5}

    def reserve_month(self,month,writes):
        key=PREFIX+f'budgets/{month}.json';ledger=self.store.read_json(key) or {'reserved_class_a':0}
        if ledger['reserved_class_a']+writes>800_000:
            raise SafetyStop('上传操作保险丝：本处理程序预留 A 类操作将超过每月80万次')
        ledger['reserved_class_a']+=writes
        self.put_json(key,ledger)
