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
import os
from boto3.s3.transfer import TransferManager, TransferConfig

from base import S3TestCase
from base import random_string, compute_md5, generate_file, wait_future_done, get_env_s3_client
from env import BUCKET

KEY_PREFIX = 'test-transfer-%s/' % random_string(8)


class TransferTest(S3TestCase):

    def __init__(self, case):
        super(TransferTest, self).__init__(case)
        self.s3 = get_env_s3_client()
        tc = TransferConfig(
            multipart_threshold=5 * 1024 * 1024,
            max_concurrency=10,
            multipart_chunksize=5 * 1024 * 1024,
            num_download_attempts=5,
            max_io_queue=100,
            io_chunksize=262144,
            use_threads=True
        )
        self.tm = TransferManager(self.s3, tc)

    def __test_transfer(self, size):
        name = random_string(16)
        key = KEY_PREFIX + name
        local_filename = os.path.join('/tmp', name)
        expect_md5 = generate_file(path=local_filename, size=size)

        # Upload parallel
        f = open(local_filename, 'rb')
        future = self.tm.upload(fileobj=f, bucket=BUCKET, key=key)
        result = wait_future_done(future, timeout=90)
        self.assertTrue(result)
        f.close()

        # Checking remote file stat
        self.assert_head_object_result(
            result=self.s3.head_object(Bucket=BUCKET, Key=key),
            content_length=size)

        # Download parallel
        download_filename = local_filename + "_dl"
        f = open(download_filename, 'wb+')
        future = self.tm.download(fileobj=f, bucket=BUCKET, key=key)
        result = wait_future_done(future, timeout=90)
        self.assertTrue(result)
        f.flush()

        # Checking download file
        f.seek(0)
        actual_md5 = compute_md5(f.read())
        f.close()
        self.assertEqual(actual_md5, expect_md5)

        # Remove remote and local files
        os.remove(local_filename)
        os.remove(download_filename)
        self.assert_delete_object_result(
            result=self.s3.delete_object(Bucket=BUCKET, Key=key))
        self.assert_delete_object_result(
            result=self.s3.delete_object(Bucket=BUCKET, Key=KEY_PREFIX))

    def test_transfer_scene1__50mb(self):
        """
        This test tests transfer (upload and download) a 50MB size file by using multipart feature.
        :return: None
        """
        self.__test_transfer(size=50 * 1024 * 1024)

    def test_transfer_scene2_100mb(self):
        """
        This test tests transfer (upload and download) a 100MB size file by using multipart feature.
        :return: None
        """
        self.__test_transfer(size=100 * 1024 * 1024)

    def test_transfer_scene3_200mb(self):
        """
        This test tests transfer (upload and download) a 200MB size file by using multipart feature.
        :return: None
        """
        self.__test_transfer(size=200 * 1024 * 1024)
