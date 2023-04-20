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

import os

from base import S3TestCase
from base import compute_md5, get_env_s3_client
from env import BUCKET, MOUNT_POINT

KEY_PREFIX = 'test-object-symlink/'


class ObjectSymlinkTest(S3TestCase):

    def __init__(self, case):
        super(ObjectSymlinkTest, self).__init__(case)
        self.s3 = get_env_s3_client()

    def setUp(self):
        self.cleanup()

    def tearDown(self):
        self.cleanup()

    def cleanup(self):
        os.chdir(MOUNT_POINT)
        os.system('rm -rf %s' % os.path.join(MOUNT_POINT, '*'))

    def test_symlink_in_path(self):
        os.chdir(MOUNT_POINT)
        os.system('mkdir -p %s' % KEY_PREFIX)
        os.chdir(os.path.join(MOUNT_POINT, KEY_PREFIX))
        for i in range(3):
            os.system('mkdir -p %02d' % i)
            os.system('echo -n %02d > %02d/meta' % (i, i))
        os.symlink('02', 'current')

        self.assert_head_bucket_result(self.s3.head_bucket(Bucket=BUCKET))
        # Test head result of key 'test-object-symlink/current'
        self.assert_head_object_result(
            result=self.s3.head_object(Bucket=BUCKET, Key=KEY_PREFIX + 'current/'),
            content_type='application/directory')
        self.assert_head_object_result(
            result=self.s3.head_object(Bucket=BUCKET, Key=KEY_PREFIX + 'current/meta'),
            content_type='application/octet-stream')
        # Test get result of key 'test-object-symlink/current' which have links to 'test-object-symlink/03/meta'.
        self.assert_get_object_result(
            result=self.s3.get_object(Bucket=BUCKET, Key=KEY_PREFIX + 'current/meta'),
            content_length=2,
            body_md5=compute_md5('02'.encode()))
        # Test delete symlink 'test-object-symlink/current'
        self.assert_delete_object_result(
            result=self.s3.delete_object(Bucket=BUCKET, Key=KEY_PREFIX + 'current')
        )
        self.assert_head_object_result(
            result=self.s3.head_object(Bucket=BUCKET, Key=KEY_PREFIX + '02/meta'),
            content_type='application/octet-stream')
        try:
            self.assert_head_object_result(
                result=self.s3.head_object(Bucket=BUCKET, Key=KEY_PREFIX + 'current/'))
            self.fail()
        except Exception as e:
            # Error code 404 is legal
            self.assert_client_error(e, expect_status_code=404)

    def test_symlink_target(self):
        os.chdir(MOUNT_POINT)
        os.system('mkdir -p %s' % KEY_PREFIX)
        os.chdir(os.path.join(MOUNT_POINT, KEY_PREFIX))
        for i in range(3):
            os.system('mkdir -p %02d' % i)
            os.system('echo -n %02d > %02d/meta' % (i, i))
        os.symlink('02/meta', 'current')

        self.assert_head_bucket_result(self.s3.head_bucket(Bucket=BUCKET))
        # Test head result of key 'test-object-symlink/current'
        self.assert_head_object_result(
            result=self.s3.head_object(Bucket=BUCKET, Key=KEY_PREFIX + 'current'),
            content_type='application/octet-stream')
        # Test get result of key 'test-object-symlink/current' which have links to 'test-object-symlink/03/meta'.
        self.assert_get_object_result(
            result=self.s3.get_object(Bucket=BUCKET, Key=KEY_PREFIX + 'current'),
            content_length=2,
            body_md5=compute_md5('02'.encode()))
        # Test delete symlink 'test-object-symlink/current'
        self.assert_delete_object_result(
            result=self.s3.delete_object(Bucket=BUCKET, Key=KEY_PREFIX + 'current')
        )
        self.assert_head_object_result(
            result=self.s3.head_object(Bucket=BUCKET, Key=KEY_PREFIX + '02/meta'),
            content_type='application/octet-stream')
        try:
            self.assert_head_object_result(
                result=self.s3.head_object(Bucket=BUCKET, Key=KEY_PREFIX + 'current'))
            self.fail()
        except Exception as e:
            # Error code 404 is legal
            self.assert_client_error(e, expect_status_code=404)

    def test_symlink_loop(self):
        os.chdir(MOUNT_POINT)
        os.system('mkdir -p %s' % KEY_PREFIX)
        os.chdir(os.path.join(MOUNT_POINT, KEY_PREFIX))

        # symlink loop 'current -> current'
        os.symlink('current', 'current')
        try:
            self.assert_head_object_result(
                result=self.s3.head_object(Bucket=BUCKET, Key=KEY_PREFIX + 'current'))
            self.fail()
        except Exception as e:
            self.s3 = get_env_s3_client()
        os.system('rm -rf %s' % os.path.join(MOUNT_POINT, KEY_PREFIX, '*'))

        # symlink loop '00 -> 01 -> 02 -> 03 -> 00'
        os.symlink('01', '00')
        os.symlink('02', '01')
        os.symlink('03', '02')
        os.symlink('00', '03')
        try:
            self.assert_head_object_result(
                result=self.s3.head_object(Bucket=BUCKET, Key=KEY_PREFIX + '00'))
            self.fail()
        except Exception as e:
            self.s3 = get_env_s3_client()
        try:
            self.assert_head_object_result(
                result=self.s3.head_object(Bucket=BUCKET, Key=KEY_PREFIX + '03'))
            self.fail()
        except Exception as e:
            self.s3 = get_env_s3_client()
        os.system('rm -rf %s' % os.path.join(MOUNT_POINT, KEY_PREFIX, '*'))

        file_index = 0
        for i in range(30):
            target = '%02d' % (i + 1)
            link = '%02d' % i
            os.symlink(target, link)
            file_index = i
        os.system('echo -n %02d > %02d' % (file_index, file_index))
        self.assert_head_object_result(
            result=self.s3.head_object(Bucket=BUCKET, Key=KEY_PREFIX + '00'))
        self.assert_get_object_result(
            result=self.s3.get_object(Bucket=BUCKET, Key=KEY_PREFIX + '00'),
            content_length=2,
            body_md5=compute_md5(('%02d' % file_index).encode()))
        os.system('rm -rf %s' % os.path.join(MOUNT_POINT, KEY_PREFIX, '*'))

        file_index = 0
        for i in range(50):
            target = '%02d' % (i + 1)
            link = '%02d' % i
            os.symlink(target, link)
            file_index = i
        os.system('echo -n %02d > %02d' % (file_index, file_index))
        try:
            self.assert_head_object_result(
                result=self.s3.head_object(Bucket=BUCKET, Key=KEY_PREFIX + '00'))
            self.fail()
        except Exception as e:
            self.s3 = get_env_s3_client()
        os.system('rm -rf %s' % os.path.join(MOUNT_POINT, KEY_PREFIX, '*'))
