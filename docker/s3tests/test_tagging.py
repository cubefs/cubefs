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

import env
from base import S3TestCase, get_env_s3_client
from base import random_string

KEY_PREFIX = 'test-tagging-%s/' % random_string(8)

empty_tag_set = []


def generate_tag_set(size=10, key_length=8, value_length=16):
    tag_set = []
    for _ in range(size):
        tag_set.append({'Key': random_string(key_length), 'Value': random_string(value_length)})
    return tag_set


def encode_tag_set(tag_set):
    encoded = ''
    for tag in tag_set:
        if len(encoded) > 0:
            encoded += '&'
        encoded += tag['Key'] + '=' + tag['Value']
    return encoded


class TaggingTest(S3TestCase):

    def __init__(self, case):
        super(TaggingTest, self).__init__(case)
        self.s3 = get_env_s3_client()

    def test_object_tagging(self):

        key = KEY_PREFIX + random_string(16)
        init_tag_set = generate_tag_set()
        result = self.s3.put_object(Bucket=env.BUCKET, Key=key, Tagging=encode_tag_set(init_tag_set))
        self.assert_put_object_result(result=result)
        etag = result['ETag']

        self.assert_head_object_result(
            result=self.s3.head_object(Bucket=env.BUCKET, Key=key),
            etag=etag,
            content_length=0)

        self.assert_get_tagging_result(
            result=self.s3.get_object_tagging(Bucket=env.BUCKET, Key=key),
            expect_tag_set=init_tag_set)

        def run():
            tag_set = generate_tag_set(size=4, key_length=8, value_length=16)

            self.assert_put_tagging_result(
                result=self.s3.put_object_tagging(
                    Bucket=env.BUCKET,
                    Key=key,
                    Tagging={'TagSet': tag_set}))

            self.assert_get_tagging_result(
                result=self.s3.get_object_tagging(Bucket=env.BUCKET, Key=key),
                expect_tag_set=tag_set)

            self.assert_delete_tagging_result(
                result=self.s3.delete_object_tagging(Bucket=env.BUCKET, Key=key))

            self.assert_get_tagging_result(
                result=self.s3.get_object_tagging(Bucket=env.BUCKET, Key=key),
                expect_tag_set=empty_tag_set)

        test_count = 50
        count = 0
        while count < test_count:
            run()
            count += 1

        # Clean up test data
        self.assert_delete_objects_result(
            result=self.s3.delete_objects(
                Bucket=env.BUCKET,
                Delete={
                    'Objects': [
                        {'Key': key},
                        {'Key': KEY_PREFIX}
                    ]
                }
            )
        )

    def test_bucket_tagging(self):

        def run():
            tag_set = generate_tag_set(size=4, key_length=8, value_length=16)

            self.assert_put_tagging_result(
                result=self.s3.put_bucket_tagging(
                    Bucket=env.BUCKET,
                    Tagging={'TagSet': tag_set}))

            self.assert_get_tagging_result(
                result=self.s3.get_bucket_tagging(Bucket=env.BUCKET),
                expect_tag_set=tag_set)

            self.assert_delete_tagging_result(
                result=self.s3.delete_bucket_tagging(Bucket=env.BUCKET))

            self.assert_get_tagging_result(
                result=self.s3.get_bucket_tagging(Bucket=env.BUCKET),
                expect_tag_set=empty_tag_set)

        test_count = 50
        count = 0
        while count < test_count:
            run()
            count += 1
