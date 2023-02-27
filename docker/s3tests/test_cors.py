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

import env
import requests
from base import S3TestCase, get_env_s3_client, random_string, random_bytes, compute_md5

PUT_ORIGIN = "www.*.com"
OTHER_ORIGIN = "*.com"

ALLOWED_HEADERS = ["*"]
ALLOWED_METHODS = ['PUT', 'POST', 'GET']
ALLOWED_ORIGINS = [PUT_ORIGIN]
EXPOSE_HEADERS = ['*']
MAX_AGE_SECONDS = 123

CORS_CONFIG = {
    'CORSRules': [
        {
            'AllowedHeaders': ALLOWED_HEADERS,
            'AllowedMethods': ALLOWED_METHODS,
            'AllowedOrigins': ALLOWED_ORIGINS,
            'ExposeHeaders': EXPOSE_HEADERS,
            'MaxAgeSeconds': MAX_AGE_SECONDS
        },
    ]
}

BUCKET_URL = '{endpoint}/{bucket_name}'.format(endpoint=env.ENDPOINT, bucket_name=env.BUCKET)


class CorsTest(S3TestCase):
    s3 = None

    def __init__(self, case):
        super(CorsTest, self).__init__(case)
        self.s3 = get_env_s3_client()

    def test_cors_set(self):
        # Get bucket CORS configuration
        self.assert_get_bucket_cors_result(
            result=self.s3.get_bucket_cors(Bucket=env.BUCKET))
        # Put bucket CORS configuration
        self.assert_result_status_code(
            result=self.s3.put_bucket_cors(Bucket=env.BUCKET, CORSConfiguration=CORS_CONFIG))
        # Get bucket CORS configuration
        self.assert_get_bucket_cors_result(
            result=self.s3.get_bucket_cors(Bucket=env.BUCKET), cors_config=CORS_CONFIG)
        # Delete bucket CORS configuration
        self.assert_result_status_code(
            result=self.s3.delete_bucket_cors(Bucket=env.BUCKET), status_code=204)
        # Get bucket CORS configuration
        self.assert_get_bucket_cors_result(
            result=self.s3.get_bucket_cors(Bucket=env.BUCKET))

    def test_cors_request(self):
        # Put bucket cors
        self.assert_result_status_code(
            result=self.s3.put_bucket_cors(Bucket=env.BUCKET, CORSConfiguration=CORS_CONFIG))
        # Send allowed cors request
        self.assert_cors_request_result(
            result=requests.get(url=BUCKET_URL, headers={'Origin': PUT_ORIGIN, 'Access-Control-Request-Method': 'GET'}),
            response_code=403,
            response_origin=PUT_ORIGIN,
            response_method='GET')
        # Send not-allowed cors request
        self.assert_cors_request_result(
            result=requests.get(url=BUCKET_URL, headers={'Origin': OTHER_ORIGIN, 'Access-Control-Request-Method': 'GET'}),
            response_code=403,
            response_origin=None,
            response_method=None)
        # Send no cors request
        self.assert_cors_request_result(
            result=requests.get(url=BUCKET_URL),
            response_code=403,
            response_origin=None,
            response_method=None)

        # Delete bucket cors
        self.assert_result_status_code(
            result=self.s3.delete_bucket_cors(Bucket=env.BUCKET), status_code=204)
        # Send cors request
        self.assert_cors_request_result(
            result=requests.get(url=BUCKET_URL, headers={'Origin': PUT_ORIGIN, 'Access-Control-Request-Method': 'GET'}),
            response_code=403,
            response_origin=PUT_ORIGIN,
            response_method='GET')

    def test_cors_options(self):
        # Put object
        key = 'test-options-object'
        size = 1024 * 256
        body = random_bytes(size)
        expect_md5 = compute_md5(body)
        self.assert_put_object_result(
            result=self.s3.put_object(Bucket=env.BUCKET, Key=key, Body=body),
            etag=expect_md5)

        # Put bucket cors
        self.assert_result_status_code(
            result=self.s3.put_bucket_cors(Bucket=env.BUCKET, CORSConfiguration=CORS_CONFIG))

        options_url = '{bucket_url}/{key}'.format(bucket_url=BUCKET_URL, key=key)

        # Send options requests
        self.assert_cors_request_result(
            result=requests.options(url=options_url,
                                    headers={'Origin': PUT_ORIGIN, 'Access-Control-Request-Method': 'GET'}),
            response_code=200,
            response_origin=PUT_ORIGIN,
            response_method='GET')

        # Delete bucket cors
        self.assert_result_status_code(
            result=self.s3.delete_bucket_cors(Bucket=env.BUCKET), status_code=204)
        # Send options requests
        self.assert_cors_request_result(
            result=requests.options(url=options_url,
                                    headers={'Origin': PUT_ORIGIN, 'Access-Control-Request-Method': 'GET'}),
            response_code=200,
            response_origin=PUT_ORIGIN,
            response_method='GET')
        # Delete object
        self.assert_delete_object_result(
            result=self.s3.delete_object(Bucket=env.BUCKET, Key=key))
