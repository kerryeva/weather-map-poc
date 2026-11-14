"""使用 SDK Stubber 验证真实 S3 参数格式；不访问网络，不使用真实凭据。"""
import hashlib,os,sys,unittest
from pathlib import Path
from unittest.mock import patch
from botocore.stub import Stubber,ANY
from botocore.exceptions import ClientError
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from storage import R2Store,SafetyStop,PREFIX

class R2Protocol(unittest.TestCase):
    def setUp(self):
        self.env=patch.dict(os.environ,{'R2_ACCOUNT_ID':'a'*32,'AWS_ACCESS_KEY_ID':'test-placeholder',
            'AWS_SECRET_ACCESS_KEY':'test-placeholder','AWS_EC2_METADATA_DISABLED':'true'})
        self.env.start();self.store=R2Store('test-bucket');self.stub=Stubber(self.store.client);self.stub.activate()
    def tearDown(self):
        self.stub.assert_no_pending_responses();self.stub.deactivate();self.env.stop()
    def test_put_uses_content_md5_and_no_multipart(self):
        data=b'forecast';key=PREFIX+'sample.pmtiles';md5=hashlib.md5(data).hexdigest()
        self.stub.add_response('put_object',{'ETag':'"'+md5+'"'},
            {'Bucket':'test-bucket','Key':key,'Body':data,'ContentLength':len(data),'ContentMD5':ANY,
             'ContentType':'application/octet-stream','IfNoneMatch':'*'})
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
    def test_forbidden_is_not_treated_as_missing(self):
        self.stub.add_client_error('get_object',service_error_code='AccessDenied',http_status_code=403,
            expected_params={'Bucket':'test-bucket','Key':PREFIX+'file.json'})
        with self.assertRaises(ClientError):self.store.read_json(PREFIX+'file.json')
    def test_bad_upload_etag_stops(self):
        self.stub.add_response('put_object',{'ETag':'"bad"'})
        with self.assertRaises(SafetyStop):self.store.put(PREFIX+'file',b'data')
    def test_object_collision_does_not_overwrite(self):
        self.stub.add_client_error('put_object',service_error_code='PreconditionFailed',http_status_code=412)
        with self.assertRaises(SafetyStop):self.store.put(PREFIX+'file',b'data',immutable=True)
    def test_failed_delete_is_not_ignored(self):
        self.stub.add_response('delete_objects',{'Errors':[{'Key':PREFIX+'old','Code':'AccessDenied'}]})
        with self.assertRaises(SafetyStop):self.store.delete([PREFIX+'old'])

if __name__=='__main__':unittest.main()
