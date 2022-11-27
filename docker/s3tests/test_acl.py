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

ACL_DEFAULT = {
    'Grants': [
        {
            'Grantee': {
                'Type': 'CanonicalUser',
            },
            'Permission': 'FULL_CONTROL'
        },
    ],
    'Owner': {
        'DisplayName': env.ACCESS_KEY,
        'ID': env.ACCESS_KEY
    }
}

ACL = {
    'Grants': [
        {
            'Grantee': {
                'DisplayName': 'xxx',
                'EmailAddress': 'xyz@aa.com',
                'ID': 'yyy',
                'Type': 'CanonicalUser',
                'URI': 'x/y/z'
            },
            'Permission': 'WRITE'
        },
    ],
    'Owner': {
        'DisplayName': 'xxx',
        'ID': 'zzz'
    }
}


class AclTest(S3TestCase):
    s3 = None

    def __init__(self, case):
        super(AclTest, self).__init__(case)
        self.s3 = get_env_s3_client()

    def test_bucket_acl(self):
        # Get bucket acl configuration
        self.assert_get_bucket_acl_result(
            result=self.s3.get_bucket_acl(Bucket=env.BUCKET), acl=ACL_DEFAULT)
        # Put bucket acl configuration
        self.assert_result_status_code(
            result=self.s3.put_bucket_acl(Bucket=env.BUCKET, AccessControlPolicy=ACL))
        # Get bucket acl configuration
        self.assert_get_bucket_acl_result(
            result=self.s3.get_bucket_acl(Bucket=env.BUCKET), acl=ACL)
        # Put bucket acl configuration
        self.assert_result_status_code(
            result=self.s3.put_bucket_acl(Bucket=env.BUCKET, AccessControlPolicy=ACL_DEFAULT))
        # Get bucket acl configuration
        self.assert_get_bucket_acl_result(
            result=self.s3.get_bucket_acl(Bucket=env.BUCKET), acl=ACL_DEFAULT)
