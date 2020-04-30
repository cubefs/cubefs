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
import boto3
import hashlib
import random
import unittest2

endpoint = 'http://object.chubao.io'
access_key = '39bEF4RrAQgMj6RV'
secret_key = 'TRL6o3JL16YOqvZGIohBDFTHZDEcFsyd'
bucket = 'ltptest'


def random_str(length):
    seed = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
    result = ''
    for i in range(length):
        result += random.choice(seed)
    return result


class TestPutObject(unittest2.TestCase):

    def __init__(self, case):
        super(TestPutObject, self).__init__(case)
        self.s3 = boto3.client('s3',
                               aws_access_key_id=access_key,
                               aws_secret_access_key=secret_key,
                               endpoint_url=endpoint)

    def test_head_bucket(self):
        response = self.s3.head_bucket(Bucket=bucket)
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)

    def test_get_bucket_location(self):
        response = self.s3.get_bucket_location(Bucket=bucket)
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)

    def test_list_buckets(self):
        response = self.s3.list_buckets()
        buckets = response['Buckets']
        self.assertTrue(len(buckets) > 0)

    def test_put_directory(self):
        key = random_str(16)
        content_type = 'application/directory'
        # put directory
        response = self.s3.put_object(Bucket=bucket, Key=key, ContentType=content_type)
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)
        # head directory
        response = self.s3.head_object(Bucket=bucket, Key=key + '/')
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)
        self.assertEqual(response['ResponseMetadata']['HTTPHeaders']['content-type'], content_type)
        self.assertEqual(response['ResponseMetadata']['HTTPHeaders']['content-length'], '0')
        # delete directory
        response = self.s3.delete_object(Bucket=bucket, Key=key + '/')
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 204)

    def __do_test_put_objects(self, file_name, file_size, file_num):
        for i in range(file_num):
            self.__do_test_put_object(file_name, file_size)

    def __do_test_put_object(self, file_name, file_size):
        key = file_name
        body = bytes(random_str(file_size), 'utf-8')
        md5 = hashlib.md5()
        md5.update(body)
        expect_etag = md5.hexdigest()
        # put object
        response = self.s3.put_object(Bucket=bucket, Key=key, Body=body)
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)
        self.assertEqual(response['ETag'], expect_etag)
        # head object
        response = self.s3.head_object(Bucket=bucket, Key=key)
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)
        self.assertEqual(response['ETag'], expect_etag)
        # get object
        response = self.s3.get_object(Bucket=bucket, Key=key)
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)
        self.assertEqual(response['ETag'], expect_etag)
        response_body = response['Body']
        response_data = response_body.read()
        md5 = hashlib.md5()
        md5.update(response_data)
        actual_etag = md5.hexdigest()
        self.assertEqual(actual_etag, expect_etag)

    def __do_test_put_objects_override(self, file_size, file_num):
        """
        Put and override the same file multiple times.
        Process:
        1. Put and override file.
        2. Delete file created by this process.
        :param file_size: numeric value, size of file (unit: byte)
        :param file_num: numeric value, number of file to put
        :return:
        """
        file_name = random_str(16)
        self.__do_test_put_objects(
            file_name=file_name,
            file_size=file_size,
            file_num=file_num)
        # delete object
        response = self.s3.delete_object(
            Bucket=bucket,
            Key=file_name)
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 204)

    def __do_test_put_objects_independent(self, file_size, file_num):
        """
        Put multiple objects with different object keys.
        Process:
        :param file_size: numeric value, size of file (unit: byte)
        :param file_num: numeric value, number of file to put
        :return:
        """
        file_names = []
        file_name_prefix = random_str(16)
        for i in range(file_num):
            file_name = "%s_%d" % (file_name_prefix, i)
            self.__do_test_put_objects(
                file_name=file_name,
                file_size=file_size,
                file_num=1)
            file_names.append(file_name)
        # check files
        matches = 0
        marker = ''
        truncated = True
        while truncated:
            response = self.s3.list_objects(Bucket=bucket, Prefix=file_name_prefix, Marker=marker, MaxKeys=100)
            self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)
            if 'Contents' in response:
                contents = response['Contents']
                matches = matches + len(contents)
            if 'NextMarker' in response:
                next_marker = response['NextMarker']
                if next_marker != '':
                    marker = next_marker
            if 'IsTruncated' in response:
                truncated = bool(response['IsTruncated'])
        self.assertEqual(matches, file_num)
        # batch delete objects
        objects = []
        for file_name in file_names:
            objects.append({"Key": file_name})
        delete = {"Objects": objects}
        response = self.s3.delete_objects(
            Bucket=bucket,
            Delete=delete)
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)
        # check deletion result
        response = self.s3.list_objects(Bucket=bucket, Prefix=file_name_prefix)
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)
        self.assertFalse('Contents' in response)

    def test_put_objects_override_256_1kb(self):
        """
        This test uploads 256 file objects with a size of 1KB (override same file)
        :return:
        """
        self.__do_test_put_objects_override(
            file_size=1024,
            file_num=256)

    def test_put_objects_override_128_10kb(self):
        """
        This test uploads 128 file objects with a size of 10KB (override same file)
        :return:
        """
        self.__do_test_put_objects_override(
            file_size=1024 * 10,
            file_num=128)

    def test_put_objects_override_64_100kb(self):
        """
        This test uploads 64 file objects with a size of 100KB (override same file)
        :return:
        """
        self.__do_test_put_objects_override(
            file_size=1024 * 100,
            file_num=64)

    def test_put_objects_override_8_1mb(self):
        """
        This test uploads 8 file objects with a size of 1MB (override same file)
        :return:
        """
        self.__do_test_put_objects_override(
            file_size=1024 * 1024,
            file_num=8)

    def test_put_objects_override_4_10mb(self):
        """
        This test uploads 4 file objects with a size of 10MB (override same file)
        :return:
        """
        self.__do_test_put_objects_override(
            file_size=1024 * 1024 * 10,
            file_num=4)

    def test_put_objects_independent_256_1kb(self):
        """
        This test uploads 256 file objects with a size of 1KB (difference file)
        :return:
        """
        self.__do_test_put_objects_independent(
            file_size=1024,
            file_num=256)

    def test_put_objects_independent_128_10kb(self):
        """
        This test uploads 128 file objects with a size of 10KB (difference file)
        :return:
        """
        self.__do_test_put_objects_independent(
            file_size=1024 * 10,
            file_num=128)

    def test_put_objects_independent_64_100kb(self):
        """
        This test uploads 64 file objects with a size of 100KB (difference file)
        :return:
        """
        self.__do_test_put_objects_independent(
            file_size=1024 * 100,
            file_num=64)

    def test_put_objects_independent_8_1mb(self):
        """
        This test uploads 8 file objects with a size of 1MB (difference file)
        :return:
        """
        self.__do_test_put_objects_independent(
            file_size=1024 * 1024,
            file_num=8)

    def test_put_objects_independent_4_10mb(self):
        """
        This test uploads 4 file objects with a size of 10MB (difference file)
        :return:
        """
        self.__do_test_put_objects_independent(
            file_size=1024 * 1024 * 10,
            file_num=4)
