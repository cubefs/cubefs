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
import re
from boto3.s3.transfer import TransferManager, TransferConfig

from base import S3TestCase
from base import random_string, compute_md5, generate_file, wait_future_done, get_env_s3_client
from env import BUCKET

KEY_PREFIX = 'test-transfer-%s/' % random_string(8)
HEADER_NAME_META_DATA = "ResponseMetadata"
HEADER_NAME_STATUS_CODE = "HTTPStatusCode"
HEADER_NAME_ETAG = "ETag"
HEADER_NAME_RANGE = "content-range"
HEADER_NAME_CONTENT_LENGTH = "ContentLength"
HEADER_NAME_MP_COUNT = "x-amz-mp-parts-count"
HEADER_NAME_HEADERS = "HTTPHeaders"


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

    def upload(self, bucket_name, object_name, body):
        self.s3.put_object(Bucket=bucket_name,
                           Key=object_name,
                           Body=body),

    def download(self, bucket_name, object_name):
        self.s3.get_object(Bucket=bucket_name,
                           Key=object_name)

    def get_object_meta(self, bucket_name, object_name, part_number=None):
        try:
            if part_number is None:
                return self.s3.head_object(Bucket=bucket_name, Key=object_name)
            else:
                return self.s3.head_object(Bucket=bucket_name, Key=object_name, PartNumber=part_number)
        except BaseException as e:
            return e.response

    @staticmethod
    def parse_part_info(part_number, part_count, part_size_const, file_size):
        # part_size, part_count, range_lower, range_upper
        last_size = file_size % part_size_const
        if part_number == part_count and last_size > 0:
            part_size = last_size
            range_upper = file_size - 1
        else:
            part_size = part_size_const
            range_upper = (part_size_const * part_number) - 1
        range_lower = part_size_const * (part_number - 1)
        return part_size, range_lower, range_upper

    @staticmethod
    def parse_part_count(part_size_const, file_size):
        part_count = file_size / part_size_const
        last_size = file_size % part_size_const
        if last_size > 0:
            part_count += 1
        return int(part_count)

    @staticmethod
    def parse_range(range_response):
        match_obj = re.match("^bytes (\\d+)-(\\d+)/(\\d+)$", range_response)
        if match_obj is None:
            return
        return int(match_obj.group(1)), int(match_obj.group(2)), int(match_obj.group(3))

    def __simulation_java_parallel_download(self, part_size_const, file_size):
        # generate file
        object_name = "java_parallel_download"
        path = os.path.join('/tmp', object_name)
        generate_file(path=path, size=file_size)

        # update file
        f = open(file=path, mode='rb')
        self.upload(bucket_name=BUCKET, object_name=object_name, body=f)
        f.close()

        # remove file
        os.remove(path)

        # calculate
        part_count = self.parse_part_count(part_size_const, file_size)
        for i in range(1, part_count + 1):
            ps, rl, ru = self.parse_part_info(part_number=i,
                                              part_count=part_count,
                                              part_size_const=part_size_const,
                                              file_size=file_size)
            head_res = self.get_object_meta(bucket_name=BUCKET, object_name=object_name, part_number=i)
            self.assertEqual(head_res[HEADER_NAME_META_DATA][HEADER_NAME_STATUS_CODE], 200)
            self.assertEqual(head_res[HEADER_NAME_CONTENT_LENGTH], ps)
            self.assertTrue(HEADER_NAME_RANGE in head_res[HEADER_NAME_META_DATA][HEADER_NAME_HEADERS])
            self.assertTrue(HEADER_NAME_MP_COUNT in head_res[HEADER_NAME_META_DATA][HEADER_NAME_HEADERS])

            range_l, range_u, file_c = self.parse_range(
                head_res[HEADER_NAME_META_DATA][HEADER_NAME_HEADERS][HEADER_NAME_RANGE])
            self.assertEqual(range_l, rl)
            self.assertEqual(range_u, ru)
            self.assertEqual(file_c, file_size)
            self.assertTrue(head_res[HEADER_NAME_ETAG].endswith("-{}".format(part_count)))

    def test_simulation_10_30M(self):
        """
        This test tests simulate java sdk to parallel download with part size 10M and file size 30M, that
        :return: None
        """
        part_size = 10 * 1024 * 1024  # 10M
        file_size = 30 * 1024 * 1024  # 30M
        self.__simulation_java_parallel_download(part_size_const=part_size, file_size=file_size)

    def test_simulation_10_35M(self):
        """
        This test tests simulate java sdk to parallel download with part size 10M and file size 35M.
        :return: None
        """
        part_size = 10 * 1024 * 1024  # 10M
        file_size = 35 * 1024 * 1024  # 35M
        self.__simulation_java_parallel_download(part_size_const=part_size, file_size=file_size)

    def test_simulation_part_number(self):
        """
        This test tests part number is bigger then part count.
        :return: None
        """
        part_size_const = 10 * 1024 * 1024  # 10M
        file_size = 30 * 1024 * 1024  # 30M
        object_name = "java_parallel_download"
        path = os.path.join('/tmp', object_name)
        generate_file(path=path, size=file_size)
        # update file
        f = open(file=path, mode='rb')
        self.upload(bucket_name=BUCKET, object_name=object_name, body=f)
        f.close()
        # remove file
        os.remove(path)
        # calculate
        part_count = self.parse_part_count(part_size_const, file_size)
        head_res = self.get_object_meta(bucket_name=BUCKET, object_name=object_name, part_number=part_count + 1)
        self.assertEqual(head_res[HEADER_NAME_META_DATA][HEADER_NAME_STATUS_CODE], 404)
