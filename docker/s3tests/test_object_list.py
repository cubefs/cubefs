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

import env
from base import S3TestCase
from base import random_string, random_bytes, get_env_s3_client

KEY_PREFIX = 'test-object-list/'


class ObjectListTest(S3TestCase):
    s3 = None

    def __init__(self, case):
        super(ObjectListTest, self).__init__(case)
        self.s3 = get_env_s3_client()

    def test_list_object_etag(self):
        file_num = 200
        files = {}  # key -> etag
        for _ in range(file_num):
            key = KEY_PREFIX + random_string(16)
            result = self.s3.put_object(Bucket=env.BUCKET, Key=key, Body=random_bytes(16))
            self.assert_put_object_result(result=result)
            files[key] = result['ETag'].strip('"')

        # check files
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
                self.assertEqual(etag, files[key])

        # clean  up test data
        objects = []
        for content in contents:
            objects.append({'Key': content['Key']})
        self.s3.delete_objects(
            Bucket=env.BUCKET,
            Delete={'Objects': objects}
        )
