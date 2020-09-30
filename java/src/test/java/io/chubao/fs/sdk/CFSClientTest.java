// Copyright 2020 The Chubao Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package io.chubao.fs.sdk;

import io.chubao.fs.sdk.exception.CFSException;
import org.junit.Assert;
import org.junit.Test;

public class CFSClientTest {
    @Test
    public void testCreateClient() {
        FileStorage storage = TestHelper.getStorage();
        Assert.assertNotNull(storage);
    }

    @Test
    public void testLackSDK() {
        String libpath = null;
        CFSClient client = TestHelper.creatClient(libpath);
        Assert.assertNull(client);
    }

    @Test
    public void testInvalidSDK() {
        String libpath = "/libcfs.so";
        CFSClient client = TestHelper.creatClient(libpath);
        Assert.assertNull(client);
    }

    @Test
    public void testLackConfig() {
        StorageConfig config = null;
        FileStorage storage = TestHelper.getStorage(config);
        Assert.assertNull(storage);
    }
}
