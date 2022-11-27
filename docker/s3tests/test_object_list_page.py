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

from os import truncate
import env
import time

from base import S3TestCase
from base import random_string, random_bytes, get_env_s3_client

KEY_PREFIX = 'test-object-list/'


class ObjectListPageTest(S3TestCase):
    '''
    '''
    s3 = None

    def __init__(self, case):
        super(ObjectListPageTest, self).__init__(case)
        self.s3 = get_env_s3_client()
        self.file_total = 0
        self.max_page_size = 17
        self.test_files = [
            {"prefix": "", "name": "abcdef0", "file_num": 3},
            {"prefix": "", "name": "abcdef1", "file_num": 4},
            {"prefix": "", "name": "abcde/f2", "file_num": 5},
            {"prefix": "a/", "name": "bcdef0", "file_num": 7},
            {"prefix": "a/", "name": "bcdef1", "file_num": 3},
            {"prefix": "ab/", "name": "cdef0", "file_num": 7},
            {"prefix": "abc/", "name": "def1", "file_num": 5},
            {"prefix": "abc/", "name": "def2", "file_num": 4},
            {"prefix": "ab/c", "name": "def02", "file_num": 5},
            {"prefix": "ab/cd", "name": "ef02", "file_num": 5},
            {"prefix": "ab/c/d", "name": "ef01", "file_num": 3},
            {"prefix": "ab/c/d", "name": "ef02", "file_num": 7},
            {"prefix": "dir1/", "name": "f1"},
            {"prefix": "dir1/", "name": "f2"},
            {"prefix": "dir1/", "name": "f3"},
            {"prefix": "dir1/", "name": "f4"},
            {"prefix": "dir1/", "name": "f5"},
            {"prefix": "dir1/", "name": "f6"},
            {"prefix": "dir1/", "name": "f7"},
            {"prefix": "dir1-2/", "name": "f1"},
            {"prefix": "dir1-2/", "name": "f2"},
            {"prefix": "dir1-2/", "name": "f3"},
            {"prefix": "dir1-2/", "name": "f4"},
            {"prefix": "dir1-2/", "name": "f5"},
            {"prefix": "dir1-2/", "name": "f6"},
            {"prefix": "dir1-2/", "name": "f7"},
            {"prefix": "dir2/", "name": "f1"},
            {"prefix": "dir2/", "name": "f2"},
            {"prefix": "dir2/", "name": "f3"},
            {"prefix": "dir2/", "name": "f4"},
            {"prefix": "dir2/", "name": "f5"},
            {"prefix": "dir2/", "name": "f6"},
            {"prefix": "dir2/", "name": "f7", "file_num": 10},
        ]

    def clear_data(self):
        prefix = ''
        marker = ''
        truncated = True
        while truncated:
            result = self.s3.list_objects(Bucket=env.BUCKET, Prefix=prefix, Marker=marker, MaxKeys=1000)
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
        self.file_total = 0

    def test_list_object_page_v1(self):
        '''
        test list object page
        '''
        files = {}  # key -> etag

        self.clear_data()

        time.sleep(5)

        file_keys = []
        prefix_keys = {}
        for test_file in self.test_files:
            for _ in range(test_file.get('file_num', 1)):
                #key = KEY_PREFIX + random_string(16)
                prefix = test_file['prefix']
                key = prefix + test_file['name'] + random_string(16)
                if not prefix_keys.get(prefix):
                    prefix_keys[prefix] = []
                prefix_keys.get(prefix).append(key)
                test_file['key'] = key
                file_keys.append({'Key': key})
                result = self.s3.put_object(Bucket=env.BUCKET, Key=key, Body=random_bytes(16))
                self.assert_put_object_result(result=result)
                files[key] = result['ETag'].strip('"')
                #print("put object key: {}".format(key))

        file_keys = sorted(file_keys, key=lambda f: f['Key'])
        last_key = file_keys[-1].get('Key')

        # print("put object key count: {}, last_key: {} ".format(len(file_keys), last_key ))

        prefixs = []
        [ prefixs.append(f.get('prefix')) for f in self.test_files if f.get('prefix') not in prefixs ]

        file_count = 0
        for prefix in prefixs:
            # print("prefix_keys: {} {}".format(prefix, prefix_keys.get(prefix, [])))
            prefix_keys = [ f['Key'] for f in file_keys if f['Key'].startswith(prefix) ]
            marker = ''
            # validate list result
            contents = []
            truncated = True
            last_marker = ''
            while truncated:
                result_contents = []
                result = self.s3.list_objects(Bucket=env.BUCKET, Prefix=prefix, Marker=marker, MaxKeys=self.max_page_size)
                self.assertEqual(result['ResponseMetadata']['HTTPStatusCode'], 200)
                if 'Contents' in result:
                    result_contents = result['Contents']
                    #print("list response content type: {}".format(type(result_contents)))
                    # self.assertTrue(type(result_contents), list)
                    contents = contents + result_contents

                if 'NextMarker' in result:
                    next_marker = result['NextMarker']
                    if next_marker != '':
                        last_marker = marker
                        marker = next_marker

                if 'IsTruncated' in result:
                    truncated = bool(result['IsTruncated'])
                else:
                    truncated = False

                if not truncated:
                    end = True

                # print("list object count: {}, maxKey: {}, prefix: {}, first_key: {}, last_key: {}, next_marker: {}, truncate: {} ".format( \
                #      len(result_contents), self.max_page_size, prefix, result_contents[0]['Key'], result_contents[-1]['Key'], marker, truncated))

                if truncated and last_marker != '':
                    # print("list object truncated: last_marker {} page_first_key: {} ".format(last_marker, result_contents[0]['Key'] ))
                    self.assertEqual(last_marker, result_contents[0]['Key'])

            # print("list object total: {}, maxKey: {}, prefix: {}, first_key: {}, last_key: {}, next_marker: {} ".format( \
            #     len(contents), self.max_page_size, prefix, contents[0]['Key'], contents[-1]['Key'], marker))

            # if prefix_keys[-1] != contents[-1]['Key']:
            #     print("{} {}", prefix_keys[-1], contents[-1]['Key'])
            self.assertEqual(prefix_keys[-1], contents[-1]['Key'])

        truncated = True
        prefix=""
        marker=""
        file_count = 0
        while truncated:
            # print("marker: {}".format(marker))
            result = self.s3.list_objects(Bucket=env.BUCKET, Prefix=prefix, Marker=marker, MaxKeys=self.max_page_size)
            truncated = bool(result.get('IsTruncated', False))
            marker=result.get('NextMarker', "")
            for content in result.get('Contents', {}):
                # print("file: {}".format(content.get('Key')))
                if not content.get('Key', "").endswith("/"):
                    file_count += 1
            # print("next_marker: {} {}".format(truncated, marker))
        self.assertEqual(file_count, len(file_keys))

        self.s3.delete_objects(
            Bucket=env.BUCKET,
            Delete={'Objects': file_keys}
        )

    def test_list_object_page_v2(self):

        self.clear_data()

        time.sleep(5)

        files = {}  # key -> etag

        file_keys = []
        prefix_keys = {}
        for test_file in self.test_files:
            for _ in range(test_file.get('file_num', 1)):
                #key = KEY_PREFIX + random_string(16)
                prefix = test_file['prefix']
                key = prefix + test_file['name'] + random_string(16)
                if not prefix_keys.get(prefix):
                    prefix_keys[prefix] = []
                prefix_keys.get(prefix).append(key)
                test_file['key'] = key
                file_keys.append({'Key': key})
                result = self.s3.put_object(Bucket=env.BUCKET, Key=key, Body=random_bytes(16))
                self.assert_put_object_result(result=result)
                files[key] = result['ETag'].strip('"')
                #print("put object key: {}".format(key))

        file_keys = sorted(file_keys, key=lambda f: f['Key'])
        last_key = file_keys[-1].get('Key')

        prefixs = []
        [ prefixs.append(f.get('prefix')) for f in self.test_files if f.get('prefix') not in prefixs ]

        # validate list result
        for prefix in prefixs:
            prefix_keys = [ f['Key'] for f in file_keys if f['Key'].startswith(prefix) ]
            contents = []
            continuation_token = ''
            last_marker = ''
            truncated = True
            while truncated:
                result = self.s3.list_objects_v2(
                    Bucket=env.BUCKET,
                    Prefix=prefix,
                    ContinuationToken=continuation_token,
                    MaxKeys=self.max_page_size)
                self.assertEqual(result['ResponseMetadata']['HTTPStatusCode'], 200)
                if 'Contents' in result:
                    result_contents = result['Contents']
                    self.assertTrue(type(result_contents), list)
                    contents = contents + result_contents
                if 'NextContinuationToken' in result:
                    next_token = result['NextContinuationToken']
                    if next_token != '':
                        last_marker = continuation_token
                        continuation_token = next_token
                if 'IsTruncated' in result:
                    truncated = bool(result['IsTruncated'])
                else:
                    truncated = False
                if truncated and last_marker != '':
                    #print("list object truncated: last_marker {} page_first_key: {} ".format(last_marker, result_contents[0]['Key'] ))
                    self.assertEqual(last_marker, result_contents[0]['Key'])

            # if prefix_keys[-1] != contents[-1]['Key']:
            #     print("{} {}", prefix_keys[-1], contents[-1]['Key'])
            self.assertEqual(prefix_keys[-1], contents[-1]['Key'])

        truncated = True
        prefix=""
        continuation_token=""
        file_count = 0
        while truncated:
            result = self.s3.list_objects_v2(
                Bucket=env.BUCKET,
                Prefix=prefix,
                ContinuationToken=continuation_token,
                MaxKeys=self.max_page_size)
            truncated = bool(result.get('IsTruncated', False))
            continuation_token=result.get('NextContinuationToken', "")
            for content in result.get('Contents', {}):
                if not content.get('Key', "").endswith("/"):
                    file_count += 1
        self.assertEqual(file_count, len(file_keys))

        self.s3.delete_objects(
            Bucket=env.BUCKET,
            Delete={'Objects': file_keys}
        )
