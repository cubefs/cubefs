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

public class SetTimesTest extends StorageTest {
    private final static Log log = LogFactory.getLog(SetTimesTest.class);

    @Test
    public void testSetDir() {
        String path = settimeTestDir + "/settimes_dir_0";
        Assert.assertTrue(mkdirs(path));
        CFSStatInfo stat = stat(path);
        Assert.assertTrue(stat != null);
        checkDirStat(stat);
        long mtime = 5000;
        long atime = 1000;

        Assert.assertTrue(setTimes(path, mtime*1000*1000*1000, atime*1000*1000*1000));
        stat = stat(path);
        Assert.assertTrue(stat != null);
        Assert.assertEquals(stat.getMtime(), mtime);

        long currTime = System.nanoTime();
        Assert.assertTrue(setTimes(path, currTime, 5000));
        stat = stat(path);
        Assert.assertTrue(stat != null);
        Assert.assertEquals(stat.getMtime(), currTime/1000/1000/1000);
        // The atime is not set
        //Assert.assertEquals(stat.getAtime(), 5000);
    }

    @Test
    public void testSetFile() {
        String path = settimeTestDir + "/settimes_file_0";
        Assert.assertTrue(mkdirs(settimeTestDir));
        Assert.assertTrue(createFile(path, 1024));
        CFSStatInfo stat = stat(path);
        Assert.assertTrue(stat != null);

        long mtime = 5000;
        long atime = 1000;
        Assert.assertTrue(setTimes(path, mtime*1000*1000*1000, atime*1000*1000*1000));
        stat = stat(path);
        Assert.assertTrue(stat != null);
        Assert.assertEquals(stat.getMtime(), mtime);
        // The atime is not set
        //Assert.assertEquals(stat.getAtime(), 5000);

        long currTime = System.nanoTime();
        Assert.assertTrue(setTimes(path, currTime, 5000));
        stat = stat(path);
        Assert.assertTrue(stat != null);
        Assert.assertEquals(stat.getMtime(), currTime/1000/1000/1000);
    }

}