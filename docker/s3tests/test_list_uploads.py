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
import uuid
from env import BUCKET
from base import S3TestCase, get_env_s3_client

KEY_PREFIX = "list/test/"
KEY_PREFIX_ONE = KEY_PREFIX + "one/"
KEY_PREFIX_TWO = KEY_PREFIX + "two/"
INIT_NUMBERS = 330
MAX_UPLOADS = 100


class UploadInfo:
    def __init__(self, upload_id, key):
        self.upload_id = upload_id
        self.key = key


class ListMultipartTest(S3TestCase):
    uploadInfos = []
    commonUploadInfos = []
    prefixUploadInfos = []
    s3 = get_env_s3_client()

    def __init__(self, case):
        super(ListMultipartTest, self).__init__(case)

    @classmethod
    def setUpClass(cls):
        """
        Init data before running test case, just create multipart upload session.
        :return:
        """
        # abort all previous multipart uploads, remove the impacts of the history data
        cls.abort_previous_upload()
        # create new test multipart uploads
        for i in range(INIT_NUMBERS):
            prefix_key = KEY_PREFIX + str(i)
            res = cls.s3.create_multipart_upload(Bucket=BUCKET, Key=prefix_key)
            upload = UploadInfo(upload_id=res["UploadId"], key=prefix_key)
            ListMultipartTest.uploadInfos.append(upload)

        for i in range(INIT_NUMBERS):
            prefix_one_key = KEY_PREFIX_ONE + str(i)
            res = cls.s3.create_multipart_upload(Bucket=BUCKET, Key=prefix_one_key)
            upload = UploadInfo(upload_id=res["UploadId"], key=prefix_one_key)
            ListMultipartTest.uploadInfos.append(upload)
            ListMultipartTest.prefixUploadInfos.append(upload)

        for i in range(INIT_NUMBERS):
            prefix_two_key = KEY_PREFIX_TWO + str(i)
            res = cls.s3.create_multipart_upload(Bucket=BUCKET, Key=prefix_two_key)
            upload = UploadInfo(upload_id=res["UploadId"], key=prefix_two_key)
            ListMultipartTest.uploadInfos.append(upload)

        for i in range(INIT_NUMBERS):
            random_key = str(uuid.uuid4())
            res = cls.s3.create_multipart_upload(Bucket=BUCKET, Key=random_key)
            upload = UploadInfo(upload_id=res["UploadId"], key=random_key)
            ListMultipartTest.uploadInfos.append(upload)
            ListMultipartTest.commonUploadInfos.append(upload)

        ListMultipartTest.uploadInfos = sorted(ListMultipartTest.uploadInfos,
                                               key=lambda upload_info: upload_info.key, )
        ListMultipartTest.prefixUploadInfos = sorted(ListMultipartTest.prefixUploadInfos,
                                                     key=lambda upload_info: upload_info.key, )
        ListMultipartTest.commonUploadInfos = sorted(ListMultipartTest.commonUploadInfos,
                                                     key=lambda upload_info: upload_info.key, )

    @classmethod
    def tearDownClass(cls):
        """
        Abort all the test data after test case finished.
        :return:
        """
        for upload_info in cls.uploadInfos:
            cls.s3.abort_multipart_upload(Bucket=BUCKET,
                                          Key=upload_info.key,
                                          UploadId=upload_info.upload_id)

    @classmethod
    def abort_previous_upload(cls):
        is_truncated = True
        key_marker = ""
        upload_id_marker = ""
        while is_truncated:
            if key_marker == "" and upload_id_marker == "":
                response = cls.s3.list_multipart_uploads(Bucket=BUCKET,
                                                         MaxUploads=MAX_UPLOADS)
            else:
                response = cls.s3.list_multipart_uploads(Bucket=BUCKET,
                                                         MaxUploads=MAX_UPLOADS,
                                                         KeyMarker=key_marker,
                                                         UploadIdMarker=upload_id_marker)
            if "Uploads" not in response or len(response["Uploads"]) <= 0:
                break
            for upload_info in response["Uploads"]:
                print("abort multipart upload,  upload id : {}, key : {}"
                      .format(upload_info["UploadId"], upload_info["Key"]))
                cls.s3.abort_multipart_upload(Bucket=BUCKET,
                                              Key=upload_info["Key"],
                                              UploadId=upload_info["UploadId"])
            is_truncated = response["IsTruncated"]
            key_marker = response["NextKeyMarker"]
            upload_id_marker = response["NextUploadIdMarker"]
            if not is_truncated:
                break

    def test_list_size_and_order(self):
        """
        check size and every key order
        1.the result list size count must equal with upload key size
        2.the result key must be ordered, and in ascending order of key
        :return:
        """
        is_truncated = True
        key_marker = ""
        upload_id_marker = ""
        response_list_size = 0
        loop_count = 0
        while is_truncated:
            if key_marker == "" and upload_id_marker == "":
                response = self.s3.list_multipart_uploads(Bucket=BUCKET,
                                                          MaxUploads=MAX_UPLOADS)
            else:
                response = self.s3.list_multipart_uploads(Bucket=BUCKET,
                                                          MaxUploads=MAX_UPLOADS,
                                                          KeyMarker=key_marker,
                                                          UploadIdMarker=upload_id_marker)
            response_list_size += len(response["Uploads"])

            if "Uploads" not in response or len(response["Uploads"]) <= 0:
                break
            for index in range(len(response["Uploads"])):
                upload_info = self.uploadInfos[(loop_count * MAX_UPLOADS) + index]
                self.assertEqual(response["Uploads"][index]["Key"], upload_info.key)
                self.assertEqual(response["Uploads"][index]["UploadId"], upload_info.upload_id)
            is_truncated = response["IsTruncated"]
            key_marker = response["NextKeyMarker"]
            upload_id_marker = response["NextUploadIdMarker"]
            loop_count = loop_count + 1
            if not is_truncated:
                break
        # check the key list size
        self.assertEqual(INIT_NUMBERS * 4, response_list_size)

    def test_prefix(self):
        """
        1. prefix list size must equal upload key list size with special prefix
        2. prefix list keys must start with special prefix : "list/test/one/"
        3. prefix list key must be in ordered, and in ascending order of key
        :return:
        """
        response = self.s3.list_multipart_uploads(Bucket=BUCKET,
                                                  Prefix=KEY_PREFIX_ONE)
        # prefix list size must equal upload key list size with special prefix
        self.assertEqual(len(response["Uploads"]), len(self.prefixUploadInfos))

        # prefix list key order must
        for index in range(len(response["Uploads"])):
            self.assertTrue(response["Uploads"][index]["Key"].startswith(KEY_PREFIX_ONE))
            self.assertEqual(response["Uploads"][index]["Key"], self.prefixUploadInfos[index].key)
            self.assertEqual(response["Uploads"][index]["UploadId"], self.prefixUploadInfos[index].upload_id)

    def test_delimiter(self):
        """
        Test delimiter, not contain prefix.
        :return:
        """
        delimiter = "/"
        response = self.s3.list_multipart_uploads(Bucket=BUCKET,
                                                  Delimiter=delimiter)
        # keys
        self.assertEqual(len(response["Uploads"]), len(self.commonUploadInfos))

        # common uploads
        for index in range(len(response["Uploads"])):
            self.assertEqual(response["Uploads"][index]["Key"], self.commonUploadInfos[index].key)
            self.assertEqual(response["Uploads"][index]["UploadId"], self.commonUploadInfos[index].upload_id)

        # prefix
        self.assertEqual(response["CommonPrefixes"][0]["Prefix"], "list/")

    def test_prefix_delimiter(self):
        """
        Test delimiter with prefix.
        :return:
        """
        delimiter = "/"
        response = self.s3.list_multipart_uploads(Bucket=BUCKET,
                                                  Prefix=KEY_PREFIX,
                                                  Delimiter=delimiter)
        # prefix
        self.assertEqual(len(response["Uploads"]), len(self.commonUploadInfos))
        self.assertEqual(len(response["CommonPrefixes"]), 2)
        self.assertEqual(response["CommonPrefixes"][0]["Prefix"], "list/test/one/")
        self.assertEqual(response["CommonPrefixes"][1]["Prefix"], "list/test/two/")
