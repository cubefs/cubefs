# Copyright 2020 The ChubaoFS Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.

import hashlib
import json
import random
import time

# -*- coding: utf-8 -*-
import boto3
import requests
from botocore.config import Config
from botocore.exceptions import ClientError
from unittest2 import TestCase

import env


def random_string(length):
    seed = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
    result = ''
    while len(result) <= length:
        result += random.choice(seed)
    return result


def random_string_cn(length):
    result = ''
    while len(result) <= length:
        result += chr(random.randint(0x4e00, 0x9fbf))
    return result


def random_bytes(length):
    """
    Generate random content with specified length
    :param length:
    :return: bytes content
    """
    f = open('/dev/urandom', 'rb')
    data = f.read(length)
    f.close()
    return data


def compute_md5(data):
    md5 = hashlib.md5()
    md5.update(data)
    return md5.hexdigest()


def generate_file(path, size):
    """
    :type path: str
    :param path: File path
    :type size: int
    :param size: File size
    :rtype string
    :return: file MD5
    """
    data = random_bytes(size)
    md5 = compute_md5(data)
    f = open(path, 'wb+', buffering=1024 * 1024)
    f.write(data)
    f.flush()
    f.close()
    return md5


def wait_future_done(future, timeout=0):
    """
    :type future: future
    :param future:
    :type timeout: int
    :param timeout:
    :rtype bool
    :return: done
    """
    loop = 0
    while True:
        if future.done() is True:
            break
        time.sleep(0.1)
        loop += 1
        if loop == 10 * timeout:
            return False
    return True


def get_env_s3_client(signature_version='s3v4'):
    return boto3.client(
        's3',
        aws_access_key_id=env.ACCESS_KEY,
        aws_secret_access_key=env.SECRET_KEY,
        endpoint_url=env.ENDPOINT,
        use_ssl=env.USE_SSL,
        config=Config(signature_version=signature_version))


def get_env_s3_client_volume_credential(signature_version='s3v4'):
    resp = requests.get(
        url=env.MASTER + '/client/vol?name=%s' % env.BUCKET,
        headers={
            'Skip-Owner-Validation': 'true'
        })
    assert resp.status_code == 200
    content = json.loads(resp.content.decode())
    assert 'code' in content
    assert content['code'] == 0
    assert ('data' in content)
    data = content['data']
    assert 'OSSSecure' in data
    credential = data['OSSSecure']
    access_key = credential['AccessKey']
    secret_key = credential['SecretKey']
    assert access_key != ''
    assert secret_key != ''
    return boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=env.ENDPOINT,
        use_ssl=env.USE_SSL,
        config=Config(signature_version=signature_version))


class S3TestCase(TestCase):

    def assert_head_bucket_result(self, result):
        self.assertNotEqual(result, None)
        self.assertEqual(type(result), dict)
        self.assertTrue('ResponseMetadata' in result)
        self.assertTrue('HTTPStatusCode' in result['ResponseMetadata'])
        self.assertEqual(result['ResponseMetadata']['HTTPStatusCode'], 200)

    def assert_get_bucket_location_result(self, result, location=None):
        self.assertNotEqual(result, None)
        self.assertEqual(type(result), dict)
        self.assertTrue('ResponseMetadata' in result)
        self.assertTrue('HTTPStatusCode' in result['ResponseMetadata'])
        self.assertEqual(result['ResponseMetadata']['HTTPStatusCode'], 200)
        if location is not None:
            self.assertTrue('LocationConstraint' in result)
            self.assertEqual(result['LocationConstraint'], location)

    def assert_get_bucket_cors_result(self, result, cors_config=None):
        self.assert_result_status_code(result=result, status_code=200)
        if cors_config is not None:
            self.assertEqual(result['CORSRules'], cors_config['CORSRules'])
        else:
            self.assertFalse('CORSRules' in result)

    def assert_cors_request_result(self, result, response_code=200, response_origin=None, response_method=None):
        self.assertEqual(result.status_code, response_code)
        self.assertEqual(result.headers.get('Access-Control-Allow-Origin'), response_origin)
        if response_method is not None:
            self.assertTrue(response_method in result.headers.get('Access-Control-Allow-Methods'))
        else:
            self.assertEqual(result.headers.get('Access-Control-Allow-Methods'), None)

    def assert_head_object_result(self, result, etag=None, content_type=None, content_length=None, metadata=None):
        self.assert_result_status_code(result=result, status_code=200)
        if etag is not None:
            self.assertEqual(result['ETag'].strip('"'), etag.strip('"'))
        if content_type is not None:
            self.assertEqual(result['ResponseMetadata']['HTTPHeaders']['content-type'], content_type)
        if content_length is not None:
            self.assertEqual(result['ResponseMetadata']['HTTPHeaders']['content-length'], str(content_length))
        if metadata is not None:
            if len(metadata) > 0:
                self.assertTrue('Metadata' in result)
                self.assertEqual(type(result['Metadata']), dict)
                self.assertEqual(result['Metadata'], metadata)
            else:
                self.assertTrue(len(result['Metadata']) == 0 if 'Metadata' in result else True)

    def assert_get_object_result(self, result, etag=None, content_type=None, content_length=None, body_md5=None):
        self.assertNotEqual(result, None)
        self.assertEqual(type(result), dict)
        self.assertTrue('ResponseMetadata' in result)
        self.assertTrue('HTTPStatusCode' in result['ResponseMetadata'])
        self.assertEqual(result['ResponseMetadata']['HTTPStatusCode'], 200)
        if etag is not None:
            self.assertEqual(result['ETag'].strip('"'), etag.strip('"'))
        if content_type is not None:
            self.assertEqual(result['ResponseMetadata']['HTTPHeaders']['content-type'], content_type)
        if content_length is not None:
            self.assertEqual(result['ResponseMetadata']['HTTPHeaders']['content-length'], str(content_length))
        if body_md5 is not None:
            self.assertTrue('Body' in result)
            body = result['Body'].read()
            self.assertEqual(compute_md5(body), body_md5)

    def assert_get_object_range_result(self, result, status_code=206, content_length=None, content_range=None):
        self.assertNotEqual(result, None)
        self.assertEqual(type(result), dict)
        self.assertTrue('ResponseMetadata' in result)
        self.assertTrue('HTTPStatusCode' in result['ResponseMetadata'])
        self.assertEqual(result['ResponseMetadata']['HTTPStatusCode'], status_code)
        if status_code == 200:
            self.assertEqual(result['ResponseMetadata']['HTTPHeaders']['accept-ranges'], 'bytes')
            if content_range is not None:
                self.assertEqual(result['ResponseMetadata']['HTTPHeaders']['content-range'], content_range)
            if content_length is not None:
                self.assertEqual(result['ResponseMetadata']['HTTPHeaders']['content-length'], str(content_length))

    def assert_put_object_result(self, result, etag=None, content_type=None, content_length=None):
        self.assertNotEqual(result, None)
        self.assertEqual(type(result), dict)
        self.assertTrue('ResponseMetadata' in result)
        self.assertTrue('HTTPStatusCode' in result['ResponseMetadata'])
        self.assertEqual(result['ResponseMetadata']['HTTPStatusCode'], 200)
        if etag is not None:
            self.assertEqual(result['ETag'].strip('"'), etag.strip('"'))
        if content_type is not None:
            self.assertEqual(result['ResponseMetadata']['HTTPHeaders']['content-type'], content_type)
        if content_length is not None:
            self.assertEqual(result['ResponseMetadata']['HTTPHeaders']['content-length'], str(content_length))

    def assert_delete_object_result(self, result):
        self.assert_result_status_code(result=result, status_code=204)

    def assert_delete_objects_result(self, result):
        self.assert_result_status_code(result, status_code=200)

    def assert_put_tagging_result(self, result):
        self.assert_result_status_code(result=result, status_code=200)

    def assert_get_tagging_result(self, result, expect_tag_set=None):
        self.assert_result_status_code(result=result, status_code=200)
        if expect_tag_set is not None:
            if len(expect_tag_set) > 0:
                self.assertTrue('TagSet' in result)
                self.assertEqual(type(result['TagSet']), list)
                self.assertEqual(len(result['TagSet']), len(expect_tag_set))
                for expect_tag in expect_tag_set:
                    self.assertTrue(expect_tag in result['TagSet'])
            else:
                self.assertTrue(len(result['TagSet']) == 0 if 'TagSet' in result else True)

    def assert_delete_tagging_result(self, result):
        self.assert_result_status_code(result=result, status_code=204)

    def assert_get_bucket_acl_result(self, result, acl):
        self.assert_result_status_code(result=result, status_code=200)
        self.assertEqual(result['Owner'], acl['Owner'])
        self.assertEqual(result['Grants'], acl['Grants'])

    def assert_get_bucket_policy_result(self, result, policy=None):
        self.assert_result_status_code(result=result, status_code=200)
        if policy is None:
            self.assertEqual(result['Policy'], 'null')
        else:
            policy_result = json.loads(result['Policy'])
            self.assertEqual(policy_result['Version'], policy['Version'])
            self.assertEqual(policy_result['Statement'][0]['Sid'], policy['Statement'][0]['Sid'])
            self.assertEqual(policy_result['Statement'][0]['Effect'], policy['Statement'][0]['Effect'])
            self.assertEqual(policy_result['Statement'][0]['Principal'], policy['Statement'][0]['Principal'])
            self.assertEqual(policy_result['Statement'][0]['Action'], policy['Statement'][0]['Action'])
            self.assertEqual(policy_result['Statement'][0]['Resource'], policy['Statement'][0]['Resource'])

    def assert_result_status_code(self, result, status_code=200):
        self.assertNotEqual(result, None)
        self.assertEqual(type(result), dict)
        self.assertTrue('ResponseMetadata' in result)
        self.assertTrue('HTTPStatusCode' in result['ResponseMetadata'])
        self.assertEqual(result['ResponseMetadata']['HTTPStatusCode'], status_code)

    def assert_client_error(self, error, exception_type=None, expect_status_code=None, expect_code=None):
        self.assertNotEqual(error, None)
        if exception_type is not None:
            self.assertEqual(type(error), exception_type)
        self.assertTrue(hasattr(error, 'response'))
        self.assertEqual(type(error.response), dict)
        if expect_status_code is not None:
            response = error.response
            self.assertTrue('ResponseMetadata' in response)
            self.assertTrue('HTTPStatusCode' in response['ResponseMetadata'])
            self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], expect_status_code)
        if expect_code is not None:
            self.assertTrue('Error' in error.response)
            self.assertTrue(type(error.response['Error']), dict)
            self.assertTrue('Code' in error.response['Error'])
            self.assertEqual(error.response['Error']['Code'], str(expect_code))
