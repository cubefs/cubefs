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

public class ChownTest extends StorageTest {
    private final static Log log = LogFactory.getLog(ChownTest.class);

    @Test
    public void testChownDir() {
        Assert.assertTrue(mkdirs(chownTestDir));
        CFSStatInfo stat = stat(chownTestDir);
        Assert.assertTrue(stat != null);
        checkDirStat(stat);
        Assert.assertTrue(chown(chownTestDir, 100, 100));
        stat = stat(chownTestDir);
        Assert.assertTrue(stat != null);
        log.info(chownTestDir + ":" + stat.toString());
        Assert.assertEquals(stat.getUid(), 100);
        Assert.assertEquals(stat.getGid(), 100);
    }

    @Test
    public void testChownFile() {
        String path = chownTestDir + "/chown_file_0";
        Assert.assertTrue(mkdirs(chownTestDir));
        Assert.assertTrue(createFile(path, 1024));
        CFSStatInfo stat = stat(path);
        Assert.assertTrue(stat != null);
        Assert.assertTrue(chown(path, 200, 200));
        stat = stat(path);
        Assert.assertTrue(stat != null);
        log.info(path + ":" + stat.toString());
        Assert.assertEquals(stat.getUid(), 200);
        Assert.assertEquals(stat.getGid(), 200);
    }
}