# Copyright 2020 The CubeFS Authors.
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
import time

import env
from base import S3TestCase
from base import random_string, random_bytes, get_env_s3_client

KEY_PREFIX = 'test-object-list/'


class ObjectListTest(S3TestCase):
    s3 = None

    def __init__(self, case):
        super(ObjectListTest, self).__init__(case)
        self.s3 = get_env_s3_client()

    def clear_data(self):
        truncated = True
        while truncated:
            result = self.s3.list_objects(Bucket=env.BUCKET, MaxKeys=1000)
            file_keys = []
            if 'Contents' in result:
                contents = result['Contents']
                for content in contents:
                    file_keys.append({'Key': content.get('Key')})
                self.s3.delete_objects(
                    Bucket=env.BUCKET,
                    Delete={'Objects': file_keys}
                )
            truncated = bool(result.get('IsTruncated', False))

    def insert_data(self, file_num=1000):
        files = {}  # key -> etag
        for _ in range(file_num):
            key = KEY_PREFIX + random_string(16)
            result = self.s3.put_object(Bucket=env.BUCKET, Key=key, Body=random_bytes(16))
            self.assert_put_object_result(result=result)
            files[key] = result['ETag'].strip('"')
        return files

    def test_list_object_v1_etag(self):
        self.clear_data()
        time.sleep(5)
        files = self.insert_data(500)

        # validate list result
        contents = []
        marker = ''
        truncated = True
        while truncated:
            result = self.s3.list_objects(Bucket=env.BUCKET, Prefix=KEY_PREFIX, Marker=marker, MaxKeys=100)
            self.assertEqual(result['ResponseMetadata']['HTTPStatusCode'], 200)
            if 'Contents' in result:
                result_contents = result['Contents']
                self.assertTrue(type(result_contents), list)
                contents = contents + result_contents
            if 'NextMarker' in result:
                next_marker = result['NextMarker']
                if next_marker != '':
                    marker = next_marker
            if 'IsTruncated' in result:
                truncated = bool(result['IsTruncated'])

        for content in contents:
            key = content['Key']
            etag = content['ETag'].strip('"')
            if not key.endswith('/'):
                # validate etag with source
                self.assertEqual(etag, files[key])
                # validate etag with head result
                self.assert_head_object_result(self.s3.head_object(Bucket=env.BUCKET, Key=key), etag=etag)

        # clean  up test data
        objects = []
        for content in contents:
            objects.append({'Key': content['Key']})
        self.s3.delete_objects(
            Bucket=env.BUCKET,
            Delete={'Objects': objects}
        )

    def test_list_object_v2_etag(self):
        self.clear_data()
        time.sleep(5)
        files = self.insert_data(500)

        # validate list result
        contents = []
        continuation_token = ''
        truncated = True
        while truncated:
            result = self.s3.list_objects_v2(
                Bucket=env.BUCKET,
                Prefix=KEY_PREFIX,
                ContinuationToken=continuation_token,
                MaxKeys=100)
            self.assertEqual(result['ResponseMetadata']['HTTPStatusCode'], 200)
            if 'Contents' in result:
                result_contents = result['Contents']
                self.assertTrue(type(result_contents), list)
                contents = contents + result_contents
            if 'NextContinuationToken' in result:
                next_token = result['NextContinuationToken']
                if next_token != '':
                    continuation_token = next_token
            if 'IsTruncated' in result:
                truncated = bool(result['IsTruncated'])
            else:
                truncated = False

        for content in contents:
            key = content['Key']
            etag = content['ETag'].strip('"')
            if not key.endswith('/'):
                # validate etag with source
                self.assertEqual(etag, files[key])
                # validate etag with head result
                self.assert_head_object_result(self.s3.head_object(Bucket=env.BUCKET, Key=key), etag=etag)

        # clean  up test data
        objects = []
        for content in contents:
            objects.append({'Key': content['Key']})
        self.s3.delete_objects(
            Bucket=env.BUCKET,
            Delete={'Objects': objects}
        )
