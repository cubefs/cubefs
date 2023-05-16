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
import json
from base import S3TestCase, get_env_s3_client

POLICY = '{' \
         '"Version": "2012-10-17", ' \
         '"Statement": [{ ' \
         '"Sid": "id-1",' \
         '"Effect": "Allow",' \
         '"Principal": {"AWS": ["arn:aws:iam::123456789012:root"]}, ' \
         '"Action": ["s3:PutObject"], ' \
         '"Resource": ["arn:aws:s3:::' + env.BUCKET + '/*" ] ' \
         '}]}'


class PolicyTest(S3TestCase):
    s3 = None

    def __init__(self, case):
        super(PolicyTest, self).__init__(case)
        self.s3 = get_env_s3_client()

    def test_policy_set(self):
        # Get bucket policy configuration
        try:
            result=self.s3.get_bucket_policy(Bucket=env.BUCKET)
        except Exception as e:
            self.assert_client_error(error=e, expect_status_code=404)
        # Put bucket policy configuration
        self.assert_result_status_code(
            result=self.s3.put_bucket_policy(Bucket=env.BUCKET, Policy=POLICY))
        # Get bucket policy configuration
        self.assert_get_bucket_policy_result(
            result=self.s3.get_bucket_policy(Bucket=env.BUCKET), policy=json.loads(POLICY))
        # Delete bucket policy configuration
        self.assert_result_status_code(
            result=self.s3.delete_bucket_policy(Bucket=env.BUCKET), status_code=204)
        # Get bucket policy configuration
        try:
            result=self.s3.get_bucket_policy(Bucket=env.BUCKET)
        except Exception as e:
            self.assert_client_error(error=e, expect_status_code=404)
