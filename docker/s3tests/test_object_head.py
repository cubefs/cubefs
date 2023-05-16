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
import datetime

import env
from base import S3TestCase
from base import random_string, random_bytes, compute_md5, get_env_s3_client

KEY_PREFIX = 'test-object-head/'


class ObjectHeadTest(S3TestCase):
    s3 = None

    def __init__(self, case):
        super(ObjectHeadTest, self).__init__(case)
        self.s3 = get_env_s3_client()

    def test_head_object(self):
        size = 1024 * 256
        self.assert_head_bucket_result(self.s3.head_bucket(Bucket=env.BUCKET))
        key = KEY_PREFIX + random_string(16)
        body = random_bytes(size)
        expect_md5 = compute_md5(body)
        self.assert_put_object_result(
            result=self.s3.put_object(Bucket=env.BUCKET, Key=key, Body=body),
            etag=expect_md5)
        self.assert_head_object_result(
            result=self.s3.head_object(Bucket=env.BUCKET, Key=key), etag=expect_md5,
            content_length=size)
        self.assert_delete_object_result(
            result=self.s3.delete_object(Bucket=env.BUCKET, Key=key))
        self.assert_delete_object_result(
            result=self.s3.delete_object(Bucket=env.BUCKET, Key=KEY_PREFIX))
        try:
            self.s3.head_object(Bucket=env.BUCKET, Key=key)
            self.fail()  # Non exception occurred is illegal.
        except Exception as e:
            # Error code 404 is legal.
            self.assert_client_error(e, expect_status_code=404)

    def test_head_object_if_match(self):
        size = 1024 * 256
        self.assert_head_bucket_result(
            result=self.s3.head_bucket(Bucket=env.BUCKET))
        key = KEY_PREFIX + random_string(16)
        body = random_bytes(size)
        expect_md5 = compute_md5(body)
        self.assert_put_object_result(
            result=self.s3.put_object(Bucket=env.BUCKET, Key=key, Body=body),
            etag=expect_md5)
        self.assert_head_object_result(
            result=self.s3.head_object(Bucket=env.BUCKET, Key=key, IfMatch=expect_md5),
            etag=expect_md5,
            content_length=size)
        try:
            fake_etag = '1b2cf535f27731c974343645a3985328'
            self.s3.head_object(Bucket=env.BUCKET, Key=key, IfMatch=fake_etag)
            self.fail()  # Non exception occurred is illegal.
        except Exception as e:
            # Error code 412 is legal.
            self.assert_client_error(error=e, expect_status_code=412)
        self.assert_delete_object_result(
            result=self.s3.delete_object(Bucket=env.BUCKET, Key=key))
        self.assert_delete_object_result(
            result=self.s3.delete_object(Bucket=env.BUCKET, Key=KEY_PREFIX))

    def test_head_object_if_modified_since(self):
        size = 1024 * 256
        self.assert_head_bucket_result(
            result=self.s3.head_bucket(Bucket=env.BUCKET))
        key = KEY_PREFIX + random_string(16)
        body = random_bytes(size)
        expect_md5 = compute_md5(body)
        self.assert_put_object_result(
            result=self.s3.put_object(Bucket=env.BUCKET, Key=key, Body=body),
            etag=expect_md5)
        self.assert_head_object_result(
            result=self.s3.head_object(
                Bucket=env.BUCKET,
                Key=key,
                IfModifiedSince=datetime.datetime(1996, 2, 14)),
            etag=expect_md5,
            content_length=size)
        try:
            self.s3.head_object(
                Bucket=env.BUCKET,
                Key=key,
                IfModifiedSince=datetime.datetime.now())
            self.fail()  # Non exception occurred is illegal.
        except Exception as e:
            # Error code 304 is legal.
            self.assert_client_error(error=e, expect_status_code=304)
        self.assert_delete_object_result(
            result=self.s3.delete_object(Bucket=env.BUCKET, Key=key))
        self.assert_delete_object_result(
            result=self.s3.delete_object(Bucket=env.BUCKET, Key=KEY_PREFIX))

    def test_head_object_if_none_match(self):
        size = 1024 * 256
        self.assert_head_bucket_result(
            result=self.s3.head_bucket(Bucket=env.BUCKET))
        key = KEY_PREFIX + random_string(16)
        body = random_bytes(size)
        expect_md5 = compute_md5(body)
        self.assert_put_object_result(
            result=self.s3.put_object(Bucket=env.BUCKET, Key=key, Body=body),
            etag=expect_md5)
        fake_etag = '1b2cf535f27731c974343645a3985328'
        self.assert_head_object_result(
            result=self.s3.head_object(
                Bucket=env.BUCKET,
                Key=key,
                IfNoneMatch=fake_etag),
            etag=expect_md5,
            content_length=size)
        try:
            self.s3.head_object(Bucket=env.BUCKET, Key=key, IfNoneMatch=expect_md5)
            self.fail()  # Non exception occurred is illegal.
        except Exception as e:
            # Error code 304 is legal.
            self.assert_client_error(error=e, expect_status_code=304)
        self.assert_delete_object_result(
            result=self.s3.delete_object(Bucket=env.BUCKET, Key=key))
        self.assert_delete_object_result(
            result=self.s3.delete_object(Bucket=env.BUCKET, Key=KEY_PREFIX))

    def test_head_object_if_unmodified_since(self):
        size = 1024 * 256
        self.assert_head_bucket_result(
            result=self.s3.head_bucket(Bucket=env.BUCKET))
        key = KEY_PREFIX + random_string(16)
        body = random_bytes(size)
        expect_md5 = compute_md5(body)
        self.assert_put_object_result(
            result=self.s3.put_object(Bucket=env.BUCKET, Key=key, Body=body),
            etag=expect_md5)
        self.assert_head_object_result(
            result=self.s3.head_object(
                Bucket=env.BUCKET,
                Key=key,
                IfUnmodifiedSince=datetime.datetime.now()),
            etag=expect_md5,
            content_length=size)
        try:
            self.s3.head_object(Bucket=env.BUCKET, Key=key, IfUnmodifiedSince=datetime.datetime(1996, 2, 14))
            self.fail()  # Non exception occurred is illegal.
        except Exception as e:
            # Error code 412 is legal.
            self.assert_client_error(
                error=e, expect_status_code=412)
        self.assert_delete_object_result(
            result=self.s3.delete_object(Bucket=env.BUCKET, Key=key))
        self.assert_delete_object_result(
            result=self.s3.delete_object(Bucket=env.BUCKET, Key=KEY_PREFIX))
