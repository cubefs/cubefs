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

public class UnlinkTest extends StorageTest {
    private final static Log log = LogFactory.getLog(UnlinkTest.class);

    @Test
    public void testUnlink() {
        String path1 = unlinkTestDir + "/f0";
        String path2 = unlinkTestDir + "/f1";
        Assert.assertTrue(mkdirs(unlinkTestDir));
        Assert.assertTrue(createFile(path1, 0));
        Assert.assertTrue(createFile(path2, 0));

        Assert.assertTrue(unlink(path1));
        Assert.assertNull(stat(path1));
        Assert.assertNotNull(stat(path2));
        Assert.assertTrue(unlink(path2));
        Assert.assertNull(stat(path2));
        Assert.assertTrue(rmdir(unlinkTestDir, true));
    }

    @Test
    public void testUnlinkDir() {
        String path1 = unlinkTestDir + "/d0";
        Assert.assertTrue(mkdirs(path1));
        Assert.assertFalse(unlink(path1));

        CFSStatInfo stat = stat(path1);
        Assert.assertNotNull(stat);
        Assert.assertTrue(rmdir(unlinkTestDir, true));
    }

    @Test
    public void testListInvalidPath() {
        String dir1 = "../";
        Assert.assertFalse(unlink(dir1));

        String dir2 = "/../";
        Assert.assertFalse(unlink(dir2));

        String dir3 = null;
        Assert.assertFalse(unlink(dir3));

        String dir4 = " ";
        Assert.assertFalse(unlink(dir4));
    }
}