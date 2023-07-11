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

# -*- coding: utf-8 -*-i
import requests

import env
from base import S3TestCase, get_env_s3_client, get_env_s3_client_volume_credential
from base import random_string, random_string_cn, random_bytes, compute_md5

KEY_PREFIX = 'test-signature-%s/' % random_string(8)


class SignatureTest(S3TestCase):

    def __test_presign(self, s3, key=None):
        """
        :type s3: botocore.client.S3
        :param s3: S3 client instance
        :return: None
        """
        # Test head a bucket by presigned url.
        url = s3.generate_presigned_url('head_bucket', Params={'Bucket': env.BUCKET})
        response = requests.head(url)
        self.assertEqual(response.status_code, 200)
        response.close()

        # Test head a bucket by expired presigned url.
        url = s3.generate_presigned_url('head_bucket', Params={'Bucket': env.BUCKET}, ExpiresIn=-1)
        response = requests.head(url)
        self.assertEqual(response.status_code, 403)
        response.close()

        # Vars for followed tests
        if key is None:
            key = KEY_PREFIX + random_string(16)
        body = random_bytes(1024)
        expect_md5 = compute_md5(body)

        # Test put an object by presigned url.
        url = s3.generate_presigned_url('put_object', Params={'Bucket': env.BUCKET, 'Key': key})
        response = requests.put(url=url, data=body)
        self.assertEqual(response.status_code, 200)
        self.assertTrue('Etag' in response.headers)
        expect_etag = response.headers['ETag']
        response.close()

        # Test head an object by presigned url.
        url = s3.generate_presigned_url('head_object', Params={'Bucket': env.BUCKET, 'Key': key})
        response = requests.head(url)
        self.assertEqual(response.status_code, 200)
        self.assertTrue('Etag' in response.headers)
        self.assertEqual(response.headers['ETag'], expect_etag)
        response.close()

        # Test get an object by presigned url.
        url = s3.generate_presigned_url('get_object', Params={'Bucket': env.BUCKET, 'Key': key})
        response = requests.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertTrue('Etag' in response.headers)
        self.assertEqual(response.headers['ETag'], expect_etag)
        actual_md5 = compute_md5(response.content)
        self.assertEqual(actual_md5, expect_md5)
        response.close()

        # Test delete an object by presigned url.
        url = s3.generate_presigned_url('delete_object', Params={'Bucket': env.BUCKET, 'Key': key})
        response = requests.delete(url)
        self.assertEqual(response.status_code, 204)
        response.close()

        # Remove key prefix
        url = s3.generate_presigned_url('delete_object', Params={'Bucket': env.BUCKET, 'Key': KEY_PREFIX})
        response = requests.delete(url)
        self.assertEqual(response.status_code, 204)
        response.close()

    def __test_sign(self, s3, key=None):
        """
        :type s3: botocore.client.S3
        :param s3: S3 client instance
        :return: None
        """
        # Test head a bucket
        self.assert_head_bucket_result(
            result=s3.head_bucket(Bucket=env.BUCKET))

        # Vars for followed tests
        if key is None:
            key = KEY_PREFIX + random_string(16)
        body = random_bytes(1024)
        expect_md5 = compute_md5(body)

        # Test put an object.
        self.assert_put_object_result(
            result=s3.put_object(Bucket=env.BUCKET, Key=key, Body=body),
            etag=expect_md5)

        # Test head an object.
        self.assert_head_object_result(
            result=s3.head_object(Bucket=env.BUCKET, Key=key),
            etag=expect_md5)

        # Test get an object.
        self.assert_get_object_result(
            result=s3.get_object(Bucket=env.BUCKET, Key=key),
            etag=expect_md5,
            content_length=1024,
            body_md5=expect_md5)

        # Test delete an object.
        self.assert_delete_object_result(
            result=s3.delete_object(Bucket=env.BUCKET, Key=key))

        # Remove key prefix.
        self.assert_delete_object_result(
            result=s3.delete_object(Bucket=env.BUCKET, Key=KEY_PREFIX))

    def test_signature_v2_en(self):
        """
        This test tests process under singed header with signature algorithm v2.
        :return: None
        """
        self.__test_sign(
            s3=get_env_s3_client())

    def test_signature_v2_cn(self):
        """
         This test tests process under singed header with signature algorithm v2.
         Key consist by Chinese characters.
         :return: None
         """
        self.__test_sign(
            s3=get_env_s3_client(),
            key=KEY_PREFIX + random_string_cn(16))

    def test_signature_v2_presign_en(self):
        """
        This test tests process under presigned url with signature algorithm v2.
        :return:
        """
        self.__test_presign(
            s3=get_env_s3_client())

    def test_signature_v2_presign_cn(self):
        """
         This test tests process under presigned url with signature algorithm v2.
         Key consist by Chinese characters.
         :return:
         """
        self.__test_presign(
            s3=get_env_s3_client(),
            key=KEY_PREFIX + random_string_cn(16))

    def test_signature_v4_en(self):
        """
        This test tests process under singed header with signature algorithm v4.
        :return: None
        """
        self.__test_sign(
            s3=get_env_s3_client())

    def test_signature_v4_cn(self):
        """
        This test tests process under singed header with signature algorithm v4.
        Key consist by Chinese characters.
        :return: None
        """
        self.__test_sign(
            s3=get_env_s3_client(),
            key=random_string_cn(16))

    def test_signature_v4_presign_en(self):
        """
        This test tests process under presigned url with signature algorithm v4.
        :return: None
        """
        self.__test_presign(
            s3=get_env_s3_client())

    def test_signature_v4_presign_cn(self):
        """
        This test tests process under presigned url with signature algorithm v4.
        Key consist by Chinese characters.
        :return: None
        """
        self.__test_presign(
            s3=get_env_s3_client(),
            key=random_string_cn(16))

    def test_signature_volume_credential(self):
        self.__test_sign(get_env_s3_client_volume_credential())
