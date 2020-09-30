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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Test;

public class CreateFileTest extends StorageTest {
    private final static Log log = LogFactory.getLog(CreateFileTest.class);

    @Test
    public void testCreateFile() {
        Assert.assertTrue(mkdirs(createTestDir));
        String path1 = createTestDir + "/f0";
        Assert.assertTrue(createFile(path1, 0L));
        CFSStatInfo stat = stat(path1);
        Assert.assertNotNull(stat);
        checkFileStat(stat);
        Assert.assertTrue(createFile(path1, 0L));
        Assert.assertTrue(rmdir(createTestDir, true));
    }

    @Test
    public void testCreateFileParentNotExist() {
        String path1 = createTestDir + "/d0/f0";
        Assert.assertFalse(createFile(path1, 0));
    }

    @Test
    public void testListInvalidPath() {
        String path1 = "../";
        Assert.assertFalse(createFile(path1, 0));

        String path2 = "/../";
        Assert.assertFalse(createFile(path2, 0L));

        String path3 = null;
        Assert.assertFalse(createFile(path3, 0L));

        String path4 = " ";
        Assert.assertFalse(createFile(path4, 0L));
    }
}