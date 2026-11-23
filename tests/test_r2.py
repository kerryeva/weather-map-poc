"""使用 SDK Stubber 验证真实 S3 参数格式；不访问网络，不使用真实凭据。"""
import hashlib,os,sys,unittest
from pathlib import Path
from unittest.mock import Mock,patch
from botocore.awsrequest import AWSResponse
from botocore.stub import Stubber,ANY
from botocore.exceptions import ClientError
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from storage import Publisher,R2Store,SafetyStop,WriteConflict,PREFIX,CONDITIONAL_WRITE_ATTEMPTS

class R2Protocol(unittest.TestCase):
    def setUp(self):
        self.env=patch.dict(os.environ,{'R2_ACCOUNT_ID':'a'*32,'AWS_ACCESS_KEY_ID':'test-placeholder',
            'AWS_SECRET_ACCESS_KEY':'test-placeholder','AWS_EC2_METADATA_DISABLED':'true'})
        self.env.start();self.store=R2Store('test-bucket');self.stub=Stubber(self.store.client);self.stub.activate()
        self.read_stub=Stubber(self.store.read_client);self.read_stub.activate()
        self.conditional_stub=Stubber(self.store.conditional_client);self.conditional_stub.activate()
    def tearDown(self):
        for stub in (self.stub,self.read_stub,self.conditional_stub):
            stub.assert_no_pending_responses();stub.deactivate()
        self.env.stop()
    def test_put_uses_content_md5_and_no_multipart(self):
        data=b'forecast';key=PREFIX+'sample.pmtiles';md5=hashlib.md5(data).hexdigest()
        self.stub.add_response('put_object',{'ETag':'"'+md5+'"'},
            {'Bucket':'test-bucket','Key':key,'Body':data,'ContentLength':len(data),'ContentMD5':ANY,
             'ContentType':'application/octet-stream','StorageClass':'STANDARD','IfNoneMatch':'*'})
        self.assertEqual(self.store.put(key,data,immutable=True),md5)
        self.assertEqual(self.store.requests['put'],1)
    def test_list_requires_all_pages(self):
        self.stub.add_response('list_objects_v2',{'IsTruncated':True,'NextContinuationToken':'next','Contents':[{'Key':'outside','Size':100,'ETag':'"abc"'}]}, {'Bucket':'test-bucket'})
        self.stub.add_response('list_objects_v2',{'IsTruncated':False,'Contents':[{'Key':PREFIX+'file','Size':200,'ETag':'"def"'}]}, {'Bucket':'test-bucket','ContinuationToken':'next'})
        result=self.store.list();self.assertEqual(sum(v['bytes'] for v in result.values()),300)
        self.assertEqual(self.store.requests['list'],2)
    def test_truncated_list_without_token_fails_closed(self):
        self.stub.add_response('list_objects_v2',{'IsTruncated':True},{'Bucket':'test-bucket'})
        with self.assertRaises(SafetyStop):self.store.list()
    def test_repeated_continuation_token_does_not_loop_forever(self):
        self.stub.add_response('list_objects_v2',{'IsTruncated':True,'NextContinuationToken':'same'},{'Bucket':'test-bucket'})
        self.stub.add_response('list_objects_v2',{'IsTruncated':True,'NextContinuationToken':'same'},{'Bucket':'test-bucket','ContinuationToken':'same'})
        with self.assertRaises(SafetyStop):self.store.list()
    def test_forbidden_is_not_treated_as_missing(self):
        self.read_stub.add_client_error('get_object',service_error_code='AccessDenied',http_status_code=403,
            expected_params={'Bucket':'test-bucket','Key':PREFIX+'file.json'})
        with self.assertRaises(ClientError):self.store.read_json(PREFIX+'file.json')
    def test_bad_upload_etag_stops(self):
        self.stub.add_response('put_object',{'ETag':'"bad"'})
        with self.assertRaises(SafetyStop):self.store.put(PREFIX+'file',b'data')
    def test_object_collision_does_not_overwrite(self):
        self.stub.add_client_error('put_object',service_error_code='PreconditionFailed',http_status_code=412)
        with self.assertRaises(SafetyStop):self.store.put(PREFIX+'file',b'data',immutable=True)
    def test_control_write_uses_conditional_create_and_replace(self):
        key=PREFIX+'available.json';data=b'{}'
        args={'Bucket':'test-bucket','Key':key,'Body':data,'ContentLength':len(data),
              'ContentMD5':ANY,'ContentType':'application/json','StorageClass':'STANDARD'}
        etag='"'+hashlib.md5(data).hexdigest()+'"'
        self.conditional_stub.add_response('put_object',{'ETag':etag},{**args,'IfNoneMatch':'*'})
        self.assertEqual(self.store.put(key,data,expected=None),etag.strip('"'))
        self.conditional_stub.add_client_error('put_object',service_error_code='PreconditionFailed',http_status_code=412,
                                   expected_params={**args,'IfMatch':'"stale"'})
        with self.assertRaises(WriteConflict):self.store.put(key,data,expected='"stale"')

    def test_request_count_is_bounded_across_sdk_and_conditional_retries(self):
        self.stub.deactivate();self.conditional_stub.deactivate()
        key=PREFIX+'available.json'
        def response(status,code):
            raw=Mock()
            raw.stream.return_value=[f'<Error><Code>{code}</Code></Error>'.encode()]
            return AWSResponse(self.store.client.meta.endpoint_url,status,{},raw)
        retryable=response(503,'SlowDown')
        regular_http=self.store.client._endpoint.http_session
        with patch.object(regular_http,'send',return_value=retryable) as send,patch('storage.time.sleep'):
            with self.assertRaises(ClientError):self.store.put(key,b'{}')
        self.assertEqual(send.call_count,2)

        conditional_http=self.store.conditional_client._endpoint.http_session
        conflict=response(412,'PreconditionFailed')
        publisher=Publisher.__new__(Publisher)
        publisher.store=Mock()
        publisher.store.read_json_version.return_value=({'count':0},'"stale"')
        publisher.put=self.store.put
        responses=[conflict,retryable,response(429,'TooManyRequests'),conflict,retryable,retryable]
        with patch.object(conditional_http,'send',side_effect=responses) as send,patch('storage.time.sleep') as pause,patch('storage.random.uniform',return_value=0):
            with self.assertRaisesRegex(SafetyStop,'持续限流'):
                publisher.update_json(key,lambda d:{'count':d['count']+1})
        self.assertEqual(send.call_count,CONDITIONAL_WRITE_ATTEMPTS)
        self.assertEqual(publisher.store.read_json_version.call_count,CONDITIONAL_WRITE_ATTEMPTS)
        self.assertEqual([call.args[0] for call in pause.call_args_list],[.2,1,2,1.6,4])
    def test_failed_delete_is_not_ignored(self):
        self.stub.add_response('delete_objects',{'Errors':[{'Key':PREFIX+'old','Code':'AccessDenied'}]})
        with self.assertRaises(SafetyStop):self.store.delete([PREFIX+'old'])

if __name__=='__main__':unittest.main()
