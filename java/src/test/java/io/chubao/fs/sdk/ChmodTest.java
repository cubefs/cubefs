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

public class ChmodTest extends StorageTest {
    private final static Log log = LogFactory.getLog(ChmodTest.class);

    @Test
    public void testchmodDir() {
        String path = chmodTestDir + "/chmod_dir_0";
        Assert.assertTrue(mkdirs(path));
        CFSStatInfo stat = stat(path);
        Assert.assertTrue(stat != null);
        checkDirStat(stat);
        Assert.assertTrue(chmod(path, 0755));
        stat = stat(path);
        Assert.assertTrue(stat != null);
        Assert.assertEquals(stat.getMode(), 0755);
    }

    @Test
    public void testchmodFile() {
        String path = chmodTestDir + "/chmod_file_0";
        Assert.assertTrue(mkdirs(chmodTestDir));
        Assert.assertTrue(createFile(path, 1024));
        CFSStatInfo stat = stat(path);
        Assert.assertTrue(stat != null);
        Assert.assertTrue(chmod(path, 0755));
        stat = stat(path);
        Assert.assertTrue(stat != null);
        Assert.assertEquals(stat.getMode(), 0755);
    }
}