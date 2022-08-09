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
import time

from env import BUCKET
from base import S3TestCase, get_env_s3_client

SOURCE_KEY = "copyTest/key/sourceKey.txt"
TARGET_KEY = "copyTest/key/targetKey.txt"

SOURCE_KEY_DIR = "copyTest/dir/targetDir/"
TARGET_KEY_DIR = "copyTest/dir/sourceDir/"

SOURCE_KEY_WITH_META = "copyTest/key/withMeta/sourceKey.txt"
TARGET_KEY_WITH_META = "copyTest/key/withMeta/targetKey.txt"

SOURCE_KEY_RESET_META = "copyTest/key/reset/sourceKey.txt"
TARGET_KEY_RESET_META = "copyTest/key/reset/targetKey.txt"

SOURCE_KEY_MODIFY_META = "copyTest/key/modify/sourceKey.txt"

META_DATE_KEY_1 = "sourceKeyMetaKey1"
META_DATE_KEY_2 = "sourceKeyMetaKey2"
META_DATE_VALUE_1 = "sourceKeyMetaValue1"
META_DATE_VALUE_2 = "sourceKeyMetaValue2"
META_DATE_VALUE_1_MODIFIED = "sourceKeyMetaValue1Modified"


class CopyObjectTest(S3TestCase):
    s3 = get_env_s3_client()

    def __init__(self, case):
        super(CopyObjectTest, self).__init__(case)

    @classmethod
    def setUpClass(cls):
        """
        Create test data, such as putting object of source keys.
        :return:
        """
        cls.clear_data()
        # create source object info
        cls.create_key(key=SOURCE_KEY, content=b'copyTest source key content')
        cls.create_key(key=SOURCE_KEY_DIR, content='')
        cls.create_key(key=SOURCE_KEY_WITH_META, content=b'copyTest source key with meta data', mete_data=True)
        cls.create_key(key=SOURCE_KEY_RESET_META, content=b'copyTest source key for used reset meta data')

    @classmethod
    def tearDownClass(cls):
        """
        Clean temp data, include initialized test data, create middle temp data and result data.
        :return:
        """
        cls.clear_data()

    @classmethod
    def create_key(cls, key, content, mete_data=False):
        """
        :return:
        """
        if mete_data:
            metadata = {META_DATE_KEY_1: META_DATE_VALUE_1, META_DATE_KEY_2: META_DATE_VALUE_2}
            cls.s3.put_object(Bucket=BUCKET, Key=key, Body=content, Metadata=metadata)
        else:
            cls.s3.put_object(Bucket=BUCKET, Key=key, Body=content)

    @classmethod
    def delete_key(cls, key):
        """
        :return:
        """
        cls.s3.delete_object(Bucket=BUCKET, Key=key)

    def __copy_object(self, s_bucket, s_key, t_bucket, t_key, is_dir=False, contain_mete_data=False):
        # sleep one second, otherwise target key last modified is same with the source
        time.sleep(1)
        copy_source = {'Bucket': s_bucket, 'Key': s_key}
        self.s3.copy_object(CopySource=copy_source, Bucket=t_bucket, Key=t_key)
        source_response = self.s3.head_object(Bucket=s_bucket, Key=s_key)
        target_response = self.s3.head_object(Bucket=t_bucket, Key=t_key)
        self.assertNotEqual(target_response["ETag"], "")
        self.assertEqual(target_response["ETag"], source_response["ETag"])
        self.assertEqual(target_response["ContentLength"], source_response["ContentLength"])
        self.assertGreater(target_response["LastModified"], source_response["LastModified"])
        if is_dir:
            self.assertEqual(target_response["ContentLength"], 0)
        if contain_mete_data:
            target_meta_data = target_response["Metadata"]
            # target object must have metadata
            # The response returned metadata key we specified is lower,
            # so when using this metadata, we need to transfer metadata key to lower
            self.assertIsNotNone(target_meta_data)
            self.assertTrue(META_DATE_KEY_1.lower() in target_meta_data.keys())
            self.assertTrue(META_DATE_KEY_2.lower() in target_meta_data.keys())
            self.assertEqual(target_meta_data[META_DATE_KEY_1.lower()], META_DATE_VALUE_1)
            self.assertEqual(target_meta_data[META_DATE_KEY_2.lower()], META_DATE_VALUE_2)

    @classmethod
    def clear_data(cls):
        cls.delete_key(key=SOURCE_KEY)
        cls.delete_key(key=TARGET_KEY)
        cls.delete_key(key=SOURCE_KEY_DIR)
        cls.delete_key(key=TARGET_KEY_DIR)
        cls.delete_key(key=SOURCE_KEY_WITH_META)
        cls.delete_key(key=TARGET_KEY_WITH_META)
        cls.delete_key(key=SOURCE_KEY_RESET_META)
        cls.delete_key(key=TARGET_KEY_RESET_META)
        cls.delete_key(key=SOURCE_KEY_MODIFY_META)

    def test_copy_common_key(self):
        """
        Copy common file, using default value.
        :return:
        """
        self.__copy_object(s_bucket=BUCKET,
                           s_key=SOURCE_KEY,
                           t_bucket=BUCKET,
                           t_key=TARGET_KEY)

    def test_copy_dir(self):
        """
        Copy directory, the source key is a directory(object content is empty, and key path has suffix '/').
        The target key is a directory too.
        :return:
        """
        self.__copy_object(s_bucket=BUCKET,
                           s_key=SOURCE_KEY_DIR,
                           t_bucket=BUCKET,
                           t_key=TARGET_KEY_DIR,
                           is_dir=True)

    def test_copy_metadata(self):
        """
        Copy source object metadata.
        If the source object has self_defined metadata, target object has its metadata too in default.
        :return:
        """
        self.__copy_object(s_bucket=BUCKET,
                           s_key=SOURCE_KEY_WITH_META,
                           t_bucket=BUCKET,
                           t_key=TARGET_KEY_WITH_META,
                           contain_mete_data=True)

    def test_copy_reset_metadata(self):
        """
        Reset target object metadata, no matter whether source object has self_defined metadata.
        :return:
        """
        source_bucket = BUCKET
        target_bucket = BUCKET
        source_key = SOURCE_KEY_RESET_META
        target_key = TARGET_KEY_RESET_META
        # sleep one second, otherwise target key last modified is same with the source
        time.sleep(1)
        copy_source = {'Bucket': source_bucket, 'Key': source_key}
        metadata = {META_DATE_KEY_1: META_DATE_VALUE_1, META_DATE_KEY_2: META_DATE_VALUE_2}
        self.s3.copy_object(CopySource=copy_source,
                            Bucket=target_bucket,
                            Key=target_key,
                            MetadataDirective="REPLACE",
                            Metadata=metadata)

        source_response = self.s3.head_object(Bucket=source_bucket, Key=source_key)
        target_response = self.s3.head_object(Bucket=target_bucket, Key=target_key)
        # compare basic info
        self.assertNotEqual(target_response["ETag"], "")
        self.assertEqual(target_response["ETag"], source_response["ETag"])
        self.assertEqual(target_response["ContentLength"], source_response["ContentLength"])
        self.assertGreater(target_response["LastModified"], source_response["LastModified"])
        # compare metadata
        # source key not contain metadata
        # target key not contain metadata
        source_metadata = source_response["Metadata"]
        target_metadata = target_response["Metadata"]
        self.assertEqual(len(source_metadata), 0)
        self.assertEqual(len(target_metadata), 2)
        self.assertTrue(META_DATE_KEY_1.lower() in target_metadata.keys())
        self.assertTrue(META_DATE_KEY_2.lower() in target_metadata.keys())
        self.assertEqual(target_metadata[META_DATE_KEY_1.lower()], META_DATE_VALUE_1)
        self.assertEqual(target_metadata[META_DATE_KEY_2.lower()], META_DATE_VALUE_2)

    def test_copy_modify_metadata(self):
        """
        Modify a object's metadata via specifying the target key has same path with source object,
        and specify new metadata value.
        :return:
        """
        metadata = {META_DATE_KEY_1: META_DATE_VALUE_1}
        content = "b'copyTest source key for used modify meta data'"
        self.s3.put_object(Bucket=BUCKET, Key=SOURCE_KEY_MODIFY_META, Body=content, Metadata=metadata)
        copy_source = {'Bucket': BUCKET, 'Key': SOURCE_KEY_MODIFY_META}
        metadata = {META_DATE_KEY_1: META_DATE_VALUE_1_MODIFIED}
        self.s3.copy_object(CopySource=copy_source,
                            Bucket=BUCKET,
                            Key=SOURCE_KEY_MODIFY_META,
                            MetadataDirective="REPLACE",
                            Metadata=metadata)
        response = self.s3.head_object(Bucket=BUCKET, Key=SOURCE_KEY_MODIFY_META)
        # target key not contain metadata
        metadata = response["Metadata"]
        self.assertEqual(len(metadata), 1)
        self.assertTrue(META_DATE_KEY_1.lower() in metadata.keys())
        self.assertEqual(metadata[META_DATE_KEY_1.lower()], META_DATE_VALUE_1_MODIFIED)
