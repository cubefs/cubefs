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

import env
from base import S3TestCase, get_env_s3_client


class BucketTest(S3TestCase):
    s3 = None

    def __init__(self, case):
        super(BucketTest, self).__init__(case)
        self.s3 = get_env_s3_client()

    def test_head_bucket(self):
        self.assert_head_bucket_result(self.s3.head_bucket(Bucket=env.BUCKET))

    def test_get_bucket_location(self):
        self.assert_get_bucket_location_result(
            result=self.s3.get_bucket_location(Bucket=env.BUCKET),
            location=env.REGION)

    def test_list_buckets(self):
        response = self.s3.list_buckets()
        buckets = response['Buckets']
        self.assertGreater(len(buckets), 0)
