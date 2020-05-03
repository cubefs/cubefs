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

# -*- coding: utf-8 -*-
import boto3
import hashlib
import json
import random
import requests
import time
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
    f = open('/dev/random', 'rb')
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
    f = open(path, 'wb+')
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

    def assert_head_object_result(self, result, etag=None, content_type=None, content_length=None):
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
        self.assertNotEqual(result, None)
        self.assertEqual(type(result), dict)
        self.assertTrue('ResponseMetadata' in result)
        self.assertTrue('HTTPStatusCode' in result['ResponseMetadata'])
        self.assertEqual(result['ResponseMetadata']['HTTPStatusCode'], 204)

    def assert_delete_objects_result(self, result):
        self.assertNotEqual(result, None)
        self.assertEqual(type(result), dict)
        self.assertTrue('ResponseMetadata' in result)
        self.assertTrue('HTTPStatusCode' in result['ResponseMetadata'])
        self.assertEqual(result['ResponseMetadata']['HTTPStatusCode'], 200)

    def assert_client_error(self, error, expect_code):
        self.assertNotEqual(error, None)
        self.assertEqual(type(error), ClientError)
        self.assertTrue(hasattr(error, 'response'))
        self.assertEqual(type(error.response), dict)
        self.assertTrue('Error' in error.response)
        self.assertTrue(type(error.response['Error']), dict)
        self.assertTrue('Code' in error.response['Error'])
        self.assertEqual(error.response['Error']['Code'], str(expect_code))
