# -*- coding: utf-8 -*-

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

import env
from base import S3TestCase
from base import random_string, random_bytes, get_env_s3_client

class ObjectListTest(S3TestCase):
    s3 = None

    def __init__(self, case):
        super(ObjectListTest, self).__init__(case)
        self.s3 = get_env_s3_client()

    def test_list_objects_v1(self):
        key_prefix = 'test-list-objects-v1/'
        file_num = 4096
        files = {}  # key -> etag
        for i in range(file_num):
            key = key_prefix + '%04d' % (i+1)
            result = self.s3.put_object(Bucket=env.BUCKET, Key=key, Body=random_bytes(16))
            self.assert_put_object_result(result=result)
            files[key] = result['ETag'].strip('"')

        # validate list all result
        contents = {}
        marker = ''
        truncated = True
        while truncated:
            result = self.s3.list_objects(Bucket=env.BUCKET, Prefix=key_prefix, Marker=marker, MaxKeys=1000)
            self.assertEqual(result['ResponseMetadata']['HTTPStatusCode'], 200)
            if 'Contents' in result:
                result_contents = result['Contents']
                self.assertTrue(type(result_contents), list)
                for result_content in result_contents:
                    key = result_content['Key']
                    if contents.get(key) is not None:
                        self.fail('duplicate key found in list result, key \'%s\', marker \'%s\'' % (key, marker))
                    if not key.endswith('/') and files.get(key) is None:
                        self.fail('unexpected key found in list result, key \'%s\', marker \'%s\'' % (key, marker))
                    contents[key] = result_content
            if 'NextMarker' in result:
                next_marker = result['NextMarker']
                if next_marker != '':
                    marker = next_marker
            if 'IsTruncated' in result:
                truncated = bool(result['IsTruncated'])

        for k in contents.keys():
            content = contents[k]
            key = content['Key']
            etag = content['ETag'].strip('"')
            if not key.endswith('/'):
                # validate etag with source
                self.assertEqual(etag, files[key])
                # validate etag with head result
                self.assert_head_object_result(self.s3.head_object(Bucket=env.BUCKET, Key=key), etag=etag)

        # validate list result with prefix '0'
        # result should be '0001' - '0999'
        contents = {}
        marker = ''
        truncated = True
        while truncated:
            result = self.s3.list_objects(Bucket=env.BUCKET, Prefix=key_prefix + '0', Marker=marker, MaxKeys=500)
            self.assertEqual(result['ResponseMetadata']['HTTPStatusCode'], 200)
            if 'Contents' in result:
                result_contents = result['Contents']
                self.assertTrue(type(result_contents), list)
                for result_content in result_contents:
                    key = result_content['Key']
                    if contents.get(key) is not None:
                        self.fail('duplicate key found in list result, key \'%s\', marker \'%s\'' % (key, marker))
                    if not key.endswith('/') and files.get(key) is None:
                        self.fail('unexpected key found in list result, key \'%s\', marker \'%s\'' % (key, marker))
                    contents[key] = result_content
            if 'NextMarker' in result:
                next_marker = result['NextMarker']
                if next_marker != '':
                    marker = next_marker
            if 'IsTruncated' in result:
                truncated = bool(result['IsTruncated'])

        self.assertEqual(999, len(contents))

        # validate list result with prefix '30'
        # result should be '3000' - '3099'
        contents = {}
        marker = ''
        truncated = True
        while truncated:
            result = self.s3.list_objects(Bucket=env.BUCKET, Prefix=key_prefix + '30', Marker=marker, MaxKeys=500)
            self.assertEqual(result['ResponseMetadata']['HTTPStatusCode'], 200)
            if 'Contents' in result:
                result_contents = result['Contents']
                self.assertTrue(type(result_contents), list)
                for result_content in result_contents:
                    key = result_content['Key']
                    if contents.get(key) is not None:
                        self.fail('duplicate key found in list result, key \'%s\', marker \'%s\'' % (key, marker))
                    if not key.endswith('/') and files.get(key) is None:
                        self.fail('unexpected key found in list result, key \'%s\', marker \'%s\'' % (key, marker))
                    contents[key] = result_content
            if 'NextMarker' in result:
                next_marker = result['NextMarker']
                if next_marker != '':
                    marker = next_marker
            if 'IsTruncated' in result:
                truncated = bool(result['IsTruncated'])

        self.assertEqual(100, len(contents))

        # validate list result with prefix '4'
        # result should be '4000' - '4096'
        contents = {}
        marker = ''
        truncated = True
        while truncated:
            result = self.s3.list_objects(Bucket=env.BUCKET, Prefix=key_prefix + '4', Marker=marker, MaxKeys=500)
            self.assertEqual(result['ResponseMetadata']['HTTPStatusCode'], 200)
            if 'Contents' in result:
                result_contents = result['Contents']
                self.assertTrue(type(result_contents), list)
                for result_content in result_contents:
                    key = result_content['Key']
                    if contents.get(key) is not None:
                        self.fail('duplicate key found in list result, key \'%s\', marker \'%s\'' % (key, marker))
                    if not key.endswith('/') and files.get(key) is None:
                        self.fail('unexpected key found in list result, key \'%s\', marker \'%s\'' % (key, marker))
                    contents[key] = result_content
            if 'NextMarker' in result:
                next_marker = result['NextMarker']
                if next_marker != '':
                    marker = next_marker
            if 'IsTruncated' in result:
                truncated = bool(result['IsTruncated'])

        self.assertEqual(97, len(contents))

        # validate list result with prefix '5'
        # result should be empty.
        contents = {}
        marker = ''
        truncated = True
        while truncated:
            result = self.s3.list_objects(Bucket=env.BUCKET, Prefix=key_prefix + '5', Marker=marker, MaxKeys=500)
            self.assertEqual(result['ResponseMetadata']['HTTPStatusCode'], 200)
            if 'Contents' in result:
                result_contents = result['Contents']
                self.assertTrue(type(result_contents), list)
                for result_content in result_contents:
                    key = result_content['Key']
                    if contents.get(key) is not None:
                        self.fail('duplicate key found in list result, key \'%s\', marker \'%s\'' % (key, marker))
                    if not key.endswith('/') and files.get(key) is None:
                        self.fail('unexpected key found in list result, key \'%s\', marker \'%s\'' % (key, marker))
                    contents[key] = result_content
            if 'NextMarker' in result:
                next_marker = result['NextMarker']
                if next_marker != '':
                    marker = next_marker
            if 'IsTruncated' in result:
                truncated = bool(result['IsTruncated'])

        self.assertEqual(0, len(contents))

        # clean  up test data
        objects = []
        for k in files.keys():
            objects.append({'Key': k})
        self.s3.delete_objects(
            Bucket=env.BUCKET,
            Delete={'Objects': objects}
        )

    def test_list_objects_v2(self):
        key_prefix = 'test-list-objects-v2/'
        file_num = 4096
        files = {}  # key -> etag
        for i in range(file_num):
            key = key_prefix + '%04d' % (i+1)
            result = self.s3.put_object(Bucket=env.BUCKET, Key=key, Body=random_bytes(16))
            self.assert_put_object_result(result=result)
            files[key] = result['ETag'].strip('"')

        # validate list result
        contents = {}
        continuation_token = ''
        truncated = True
        while truncated:
            result = self.s3.list_objects_v2(
                Bucket=env.BUCKET,
                Prefix=key_prefix,
                ContinuationToken=continuation_token,
                MaxKeys=1000)
            self.assertEqual(result['ResponseMetadata']['HTTPStatusCode'], 200)
            if 'Contents' in result:
                result_contents = result['Contents']
                self.assertTrue(type(result_contents), list)
                for result_content in result_contents:
                    key = result_content['Key']
                    if contents.get(key) is not None:
                        self.fail('duplicate key found in list result, key \'%s\', marker \'%s\'' % (key, marker))
                    if not key.endswith('/') and files.get(key) is None:
                        self.fail('unexpected key found in list result, key \'%s\', marker \'%s\'' % (key, marker))
                    contents[key] = result_content
            if 'NextContinuationToken' in result:
                next_token = result['NextContinuationToken']
                if next_token != '':
                    continuation_token = next_token
            if 'IsTruncated' in result:
                truncated = bool(result['IsTruncated'])
            else:
                truncated = False

        for k in contents.keys():
            content = contents[k]
            key = content['Key']
            etag = content['ETag'].strip('"')
            if not key.endswith('/'):
                # validate etag with source
                self.assertEqual(etag, files[key])
                # validate etag with head result
                self.assert_head_object_result(self.s3.head_object(Bucket=env.BUCKET, Key=key), etag=etag)

        # validate list result with prefix '0'
        # result should be '0001' - '0999'
        contents = {}
        continuation_token = ''
        truncated = True
        while truncated:
            result = self.s3.list_objects_v2(
                Bucket=env.BUCKET,
                Prefix=key_prefix + '0',
                ContinuationToken=continuation_token,
                MaxKeys=500)
            self.assertEqual(result['ResponseMetadata']['HTTPStatusCode'], 200)
            if 'Contents' in result:
                result_contents = result['Contents']
                self.assertTrue(type(result_contents), list)
                for result_content in result_contents:
                    key = result_content['Key']
                    if contents.get(key) is not None:
                        self.fail('duplicate key found in list result, key \'%s\', marker \'%s\'' % (key, marker))
                    if not key.endswith('/') and files.get(key) is None:
                        self.fail('unexpected key found in list result, key \'%s\', marker \'%s\'' % (key, marker))
                    contents[key] = result_content
            if 'NextContinuationToken' in result:
                next_token = result['NextContinuationToken']
                if next_token != '':
                    continuation_token = next_token
            if 'IsTruncated' in result:
                truncated = bool(result['IsTruncated'])
            else:
                truncated = False

        self.assertEqual(999, len(contents))

        # validate list result with prefix '30'
        # result should be '3000' - '3099'
        contents = {}
        continuation_token = ''
        truncated = True
        while truncated:
            result = self.s3.list_objects_v2(
                Bucket=env.BUCKET,
                Prefix=key_prefix + '30',
                ContinuationToken=continuation_token,
                MaxKeys=500)
            self.assertEqual(result['ResponseMetadata']['HTTPStatusCode'], 200)
            if 'Contents' in result:
                result_contents = result['Contents']
                self.assertTrue(type(result_contents), list)
                for result_content in result_contents:
                    key = result_content['Key']
                    if contents.get(key) is not None:
                        self.fail('duplicate key found in list result, key \'%s\', marker \'%s\'' % (key, marker))
                    if not key.endswith('/') and files.get(key) is None:
                        self.fail('unexpected key found in list result, key \'%s\', marker \'%s\'' % (key, marker))
                    contents[key] = result_content
            if 'NextContinuationToken' in result:
                next_token = result['NextContinuationToken']
                if next_token != '':
                    continuation_token = next_token
            if 'IsTruncated' in result:
                truncated = bool(result['IsTruncated'])
            else:
                truncated = False

        self.assertEqual(100, len(contents))

        # validate list result with prefix '4'
        # result should be '4000' - '4096'
        contents = {}
        continuation_token = ''
        truncated = True
        while truncated:
            result = self.s3.list_objects_v2(
                Bucket=env.BUCKET,
                Prefix=key_prefix + '4',
                ContinuationToken=continuation_token,
                MaxKeys=500)
            self.assertEqual(result['ResponseMetadata']['HTTPStatusCode'], 200)
            if 'Contents' in result:
                result_contents = result['Contents']
                self.assertTrue(type(result_contents), list)
                for result_content in result_contents:
                    key = result_content['Key']
                    if contents.get(key) is not None:
                        self.fail('duplicate key found in list result, key \'%s\', marker \'%s\'' % (key, marker))
                    if not key.endswith('/') and files.get(key) is None:
                        self.fail('unexpected key found in list result, key \'%s\', marker \'%s\'' % (key, marker))
                    contents[key] = result_content
            if 'NextContinuationToken' in result:
                next_token = result['NextContinuationToken']
                if next_token != '':
                    continuation_token = next_token
            if 'IsTruncated' in result:
                truncated = bool(result['IsTruncated'])
            else:
                truncated = False

        self.assertEqual(97, len(contents))

        # validate list result with prefix '5'
        # result should be empty.
        contents = {}
        continuation_token = ''
        truncated = True
        while truncated:
            result = self.s3.list_objects_v2(
                Bucket=env.BUCKET,
                Prefix=key_prefix + '5',
                ContinuationToken=continuation_token,
                MaxKeys=500)
            self.assertEqual(result['ResponseMetadata']['HTTPStatusCode'], 200)
            if 'Contents' in result:
                result_contents = result['Contents']
                self.assertTrue(type(result_contents), list)
                for result_content in result_contents:
                    key = result_content['Key']
                    if contents.get(key) is not None:
                        self.fail('duplicate key found in list result, key \'%s\', marker \'%s\'' % (key, marker))
                    if not key.endswith('/') and files.get(key) is None:
                        self.fail('unexpected key found in list result, key \'%s\', marker \'%s\'' % (key, marker))
                    contents[key] = result_content
            if 'NextContinuationToken' in result:
                next_token = result['NextContinuationToken']
                if next_token != '':
                    continuation_token = next_token
            if 'IsTruncated' in result:
                truncated = bool(result['IsTruncated'])
            else:
                truncated = False

        self.assertEqual(0, len(contents))

        # clean  up test data
        objects = []
        for k in files.keys():
            objects.append({'Key': k})
        self.s3.delete_objects(
            Bucket=env.BUCKET,
            Delete={'Objects': objects}
        )
