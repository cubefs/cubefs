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

from base import S3TestCase
from base import random_string, random_bytes, compute_md5, get_env_s3_client
from env import BUCKET

KEY_PREFIX = 'test-object-get-range/'


class ObjectGetRangeTest(S3TestCase):
    '''
    '''
    s3 = None

    def __init__(self, case):
        super(ObjectGetRangeTest, self).__init__(case)
        self.s3 = get_env_s3_client()
        self.file_size = 10000
        self.file_key = KEY_PREFIX + random_string(16)
        self.test_cases = [
            { "range":"bytes=0-499", "status_code": 206, "content-range":"bytes 0-499/10000", "content-length": 500 },
            { "range":"bytes=500-999", "status_code": 206, "content-range":"bytes 500-999/10000",  "content-length": 500 },
            { "range":"bytes=9500-", "status_code": 206, "content-range":"bytes 9500-9999/10000",  "content-length": 500 },
            { "range":"bytes=0-", "status_code": 206, "content-range":"bytes 0-9999/10000",  "content-length": 10000 },
            { "range":"bytes=0-0", "status_code": 206, "content-range":"bytes 0-0/10000",  "content-length": 1 },
            { "range":"bytes=-500", "status_code": 206, "content-range":"bytes 9500-9999/10000", "content-length": 500 },
            { "range":"bytes=-1", "status_code": 206, "content-range":"bytes 9999-9999/10000",  "content-length": 1 },
            { "range":"bytes=-0", "status_code": 206, "content-range":"bytes 0-9999/10000", "content-length": 10000 },
            { "range":"bytes=1-0", "status_code": 416 },
            { "range":"bytes=10", "status_code": 416 },
            { "range":"bytes=", "status_code": 416 },
            { "range":"bytes=abc", "status_code": 416 },
            { "range":"bytes=abc-123", "status_code": 416 },
            { "range":"1-0", "status_code": 416 },
        ]

        self._init_object()

    def _init_object(self):
        file_keys = []
        result = self.s3.list_objects(Bucket=BUCKET, Prefix=KEY_PREFIX)
        if 'Contents' in result:
            contents = result['Contents']
            for content in contents:
                file_keys.append({'Key': content.get('Key')})

        if len(file_keys) > 0:
            self.s3.delete_objects(
                Bucket=BUCKET,
                Delete={'Objects': file_keys}
            )

        self.s3.put_object(Bucket=BUCKET, Key=self.file_key, Body=random_bytes(self.file_size))

    def test_get_object_range(self):
        '''
        test get object range
        '''
        for test_case in self.test_cases:
            try:
                result=self.s3.get_object(Bucket=BUCKET, Key=self.file_key, Range=test_case.get('range')),
            except Exception as e:
                self.assert_client_error(error=e, expect_status_code=test_case.get('status_code'))
            else:
                self.assert_get_object_range_result(
                    result=result[0],
                    status_code=test_case.get('status_code'),
                    content_range=test_case.get('content-range', None),
                    content_length=test_case.get('content-length', None)
                )

        self.s3.delete_object( Bucket=BUCKET, Key=self.file_key)

