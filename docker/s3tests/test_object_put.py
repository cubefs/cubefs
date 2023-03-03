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

from base import S3TestCase
from base import random_string, random_bytes, compute_md5, get_env_s3_client
from env import BUCKET

KEY_PREFIX = 'test-object-put/'


class ObjectPutTest(S3TestCase):

    def __init__(self, case):
        super(ObjectPutTest, self).__init__(case)
        self.s3 = get_env_s3_client()

    def test_put_directory(self):
        key = random_string(16)
        content_type = 'application/directory'
        md5 = compute_md5(bytes())
        # Put a directory object
        self.assert_put_object_result(
            result=self.s3.put_object(Bucket=BUCKET, Key=key, ContentType=content_type),
            etag=md5)
        # Get the directory info
        self.assert_head_object_result(
            result=self.s3.head_object(Bucket=BUCKET, Key=key + '/'),
            etag=md5,
            content_type=content_type,
            content_length=0)
        # Get directory object
        self.assert_get_object_result(
            result=self.s3.get_object(Bucket=BUCKET, Key=key + '/'),
            etag=md5,
            content_type=content_type,
            content_length=0,
            body_md5=md5)
        # Delete the directory
        self.assert_delete_object_result(
            result=self.s3.delete_object(Bucket=BUCKET, Key=key + '/'))

    def __do_test_put_objects(self, file_name, file_size, file_num):
        for _ in range(file_num):
            self.__do_test_put_object(file_name, file_size)

    def __do_test_put_object(self, file_name, file_size):
        key = file_name
        body = random_bytes(file_size)
        expect_etag = compute_md5(body)
        # put object
        self.assert_put_object_result(
            result=self.s3.put_object(Bucket=BUCKET, Key=key, Body=body),
            etag=expect_etag)
        # head object
        self.assert_head_object_result(
            result=self.s3.head_object(Bucket=BUCKET, Key=key),
            etag=expect_etag,
            content_length=file_size)
        # get object
        self.assert_get_object_result(
            result=self.s3.get_object(Bucket=BUCKET, Key=key),
            etag=expect_etag,
            content_length=file_size,
            body_md5=expect_etag)

    def __do_test_put_objects_override(self, file_size, file_num):
        """
        Put and override the same file multiple times.
        Process:
        1. Put and override file.
        2. Delete file created by this process.
        :type file_size: int
        :type file_num: int
        :param file_size: numeric value, size of file (unit: byte)
        :param file_num: numeric value, number of file to put
        :return: None
        """
        file_name = random_string(16)
        self.__do_test_put_objects(
            file_name=file_name,
            file_size=file_size,
            file_num=file_num)
        # delete object
        self.assert_delete_object_result(
            result=self.s3.delete_object(Bucket=BUCKET, Key=file_name))

    def __do_test_put_objects_independent(self, file_size, file_num):
        """
        Put multiple objects with different object keys.
        Process:
        :type file_size: int
        :type file_num: int
        :param file_size: numeric value, size of file (unit: byte)
        :param file_num: numeric value, number of file to put
        :return:
        """
        file_names = []
        file_name_prefix = random_string(16)
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
            response = self.s3.list_objects(Bucket=BUCKET, Prefix=file_name_prefix, Marker=marker, MaxKeys=100)
            self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)
            if 'Contents' in response:
                contents = response['Contents']
                self.assertTrue(type(contents), list)
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
        self.assert_delete_objects_result(
            result=self.s3.delete_objects(Bucket=BUCKET, Delete=delete))
        # check deletion result
        response = self.s3.list_objects(Bucket=BUCKET, Prefix=file_name_prefix)
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)
        self.assertFalse('Contents' in response)

    def test_put_objects_override_scene1___1kb(self):
        """
        This test uploads 1 file object with a size of 1KB (override same file)
        :return:
        """
        self.__do_test_put_objects_override(
            file_size=1024,
            file_num=1)

    def test_put_objects_override_scene2__10kb(self):
        """
        This test uploads 1 file object with a size of 10KB (override same file)
        :return:
        """
        self.__do_test_put_objects_override(
            file_size=1024 * 10,
            file_num=1)

    def test_put_objects_override_scene3_100kb(self):
        """
        This test uploads 1 file object with a size of 100KB (override same file)
        :return:
        """
        self.__do_test_put_objects_override(
            file_size=1024 * 100,
            file_num=1)

    def test_put_objects_override_scene4___1mb(self):
        """
        This test uploads 1 file object with a size of 1MB (override same file)
        :return:
        """
        self.__do_test_put_objects_override(
            file_size=1024 * 1024,
            file_num=1)

    def test_put_objects_override_scene5__10mb(self):
        """
        This test uploads 1 file object with a size of 10MB (override same file)
        :return:
        """
        self.__do_test_put_objects_override(
            file_size=1024 * 1024 * 10,
            file_num=1)

    def test_put_objects_independent_scene1___1kb(self):
        """
        This test uploads 200 file objects with a size of 1KB (difference file)
        :return:
        """
        self.__do_test_put_objects_independent(
            file_size=1024,
            file_num=200)

    def test_put_objects_independent_scene2__10kb(self):
        """
        This test uploads 100 file objects with a size of 10KB (difference file)
        :return:
        """
        self.__do_test_put_objects_independent(
            file_size=1024 * 10,
            file_num=100)

    def test_put_objects_independent_scene3_100kb(self):
        """
        This test uploads 50 file objects with a size of 100KB (difference file)
        :return:
        """
        self.__do_test_put_objects_independent(
            file_size=1024 * 100,
            file_num=50)

    def test_put_objects_independent_scene4___1mb(self):
        """
        This test uploads 10 file objects with a size of 1MB (difference file)
        :return:
        """
        self.__do_test_put_objects_independent(
            file_size=1024 * 1024,
            file_num=10)

    def test_put_objects_independent_scene5__10mb(self):
        """
        This test uploads 5 file objects with a size of 10MB (difference file)
        :return:
        """
        self.__do_test_put_objects_independent(
            file_size=1024 * 1024 * 10,
            file_num=5)

    def test_put_object_conflict_scene1(self):
        """
        This test tests response when target key exists but file mode conflict with expect.
        :return:
        """
        key = KEY_PREFIX + random_string(16)
        directory_mime = 'application/directory'
        directory_etag = 'd41d8cd98f00b204e9800998ecf8427e'
        self.assert_put_object_result(
            result=self.s3.put_object(Bucket=BUCKET, Key=key, ContentType=directory_mime),
            etag=directory_etag)
        self.assert_head_object_result(
            result=self.s3.head_object(Bucket=BUCKET, Key=key + '/'),
            etag=directory_etag,
            content_type=directory_mime)
        try:
            self.s3.put_object(Bucket=BUCKET, Key=key)
            self.fail()
        except Exception as e:
            self.assert_client_error(error=e, expect_status_code=409)
            pass
        finally:
            self.assert_delete_objects_result(
                result=self.s3.delete_objects(
                    Bucket=BUCKET,
                    Delete={
                        'Objects': [
                            {'Key': key + '/'},
                            {'Key': KEY_PREFIX}
                        ]
                    }
                )
            )

    def test_put_object_conflict_scene2(self):
        """
        This test tests response when target key exists but file mode conflict with expect.
        :return:
        """
        key = KEY_PREFIX + random_string(16)
        empty_content_etag = 'd41d8cd98f00b204e9800998ecf8427e'
        self.assert_put_object_result(
            result=self.s3.put_object(Bucket=BUCKET, Key=key),
            etag=empty_content_etag)
        self.assert_head_object_result(
            result=self.s3.head_object(Bucket=BUCKET, Key=key),
            etag=empty_content_etag)
        try:
            directory_mime = 'application/directory'
            self.s3.put_object(Bucket=BUCKET, Key=key, ContentType=directory_mime)
            self.fail()
        except Exception as e:
            self.assert_client_error(error=e, expect_status_code=409)
            pass
        finally:
            self.assert_delete_objects_result(
                result=self.s3.delete_objects(
                    Bucket=BUCKET,
                    Delete={
                        'Objects': [
                            {'Key': key},
                            {'Key': KEY_PREFIX}
                        ]
                    }
                )
            )

    def test_put_object_with_metadata(self):
        """
        This test tests put an object with user-defined metadata.
        Test count: 10
        :return:
        """

        def run():
            key = KEY_PREFIX + random_string(16)
            metadata = {
                random_string(8).lower(): random_string(16),
                random_string(8).lower(): random_string(16)
            }
            self.assert_put_object_result(
                result=self.s3.put_object(
                    Bucket=BUCKET,
                    Key=key,
                    Metadata=metadata))
            self.assert_head_object_result(
                result=self.s3.head_object(Bucket=BUCKET, Key=key),
                metadata=metadata
            )
            self.assert_delete_objects_result(
                result=self.s3.delete_objects(
                    Bucket=BUCKET, Delete={'Objects': [{'Key': key}, {'Key': KEY_PREFIX}]}))

        test_count = 10
        for _ in range(test_count):
            run()
