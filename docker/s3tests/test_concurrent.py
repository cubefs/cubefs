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
import threading

from base import S3TestCase
from base import get_env_s3_client
from env import BUCKET

KEY_PREFIX = 'test-object-put-get-concurrent/'


class ConcurrentTest(S3TestCase):

    def __init__(self, case):
        super(ConcurrentTest, self).__init__(case)
        self.s3 = get_env_s3_client()

    def test_put_get_concurrent(self):
        key = KEY_PREFIX + 'data'
        body = 'test'.encode()
        # put object
        self.assert_put_object_result(
            result=self.s3.put_object(Bucket=BUCKET, Key=key, Body=body))
        # head object
        self.assert_head_object_result(
            result=self.s3.head_object(Bucket=BUCKET, Key=key))

        run_time_sec = 30

        class FailCounter:

            def __init__(self):
                pass

            lock = threading.Lock()
            count = 0

            def increase(self):
                self.lock.acquire()
                self.count += 1
                self.lock.release()

            def get(self):
                self.lock.acquire()
                result = self.count
                self.lock.release()
                return result

        class PutThread(threading.Thread):

            def __init__(self, s3):
                super(PutThread, self).__init__()
                self.s3 = s3

            def run(self):
                start = int(datetime.datetime.now().timestamp())
                while True:
                    now = int(datetime.datetime.now().timestamp())
                    if now - start > run_time_sec:
                        return
                    self.s3.put_object(Bucket=BUCKET, Key=key, Body=body)

        class GetThread(threading.Thread):

            def __init__(self, s3, counter):
                super(GetThread, self).__init__()
                self.s3 = s3
                self.counter = counter

            def run(self):
                start = int(datetime.datetime.now().timestamp())
                while True:
                    now = int(datetime.datetime.now().timestamp())
                    if now - start > run_time_sec:
                        return
                    try:
                        self.s3.get_object(Bucket=BUCKET, Key=key)
                    except Exception as e:
                        self.counter.increase()
                        return

        threads = []
        fail_counter = FailCounter()
        get_thread_count = 16
        for i in range(get_thread_count):
            threads.append(GetThread(self.s3, fail_counter))
        put_thread_count = 16
        for i in range(put_thread_count):
            threads.append(PutThread(self.s3))

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        self.assertEqual(fail_counter.get(), 0)
