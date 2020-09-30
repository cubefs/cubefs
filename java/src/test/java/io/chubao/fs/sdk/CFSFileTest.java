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

public class CFSFileTest extends StorageTest {
    private final static Log log = LogFactory.getLog(CFSFileTest.class);

    @Test
    public void testCreate() {
        String path = cfileTestDir + "/f0";
        Assert.assertTrue(mkdirs(cfileTestDir));
        long size = 2048;
        Assert.assertTrue(createFile(path, size));
        Assert.assertTrue(unlink(path));
    }

    @Test
    public void testAppend() {
        String path = cfileTestDir + "/f1";
        Assert.assertTrue(mkdirs(cfileTestDir));
        long size = 2048;
        Assert.assertTrue(createFile(path, size));
        size = 4096;
        Assert.assertTrue(appendFile(path, size));
        Assert.assertTrue(unlink(path));
    }

    @Test
    public void testTruncate() {
        String path = cfileTestDir + "/f2";
        Assert.assertTrue(mkdirs(cfileTestDir));
        long size = 2048;
        Assert.assertTrue(createFile(path, size));
        size = 4096;
        Assert.assertTrue(truncateFile(path, size));
        Assert.assertTrue(unlink(path));
    }


    @Test
    public void testSeek() {
        String path = cfileTestDir + "/f3";
        long size = 2048;
        Assert.assertTrue(mkdirs(cfileTestDir));
        Assert.assertTrue(createFile(path, size));
        CFSFile cfile = openFile(path, FileStorage.O_WRONLY);
        long offset = 1;
        Assert.assertTrue(seek(cfile, offset));

        byte[] buff = genBuff(buffSize);
        for (int i = 0; i < size / buffSize; i++) {
            write(cfile, buff, 0, buffSize);
        }
        Assert.assertEquals(cfile.getFileSize(), size + offset);
        close(cfile);
        CFSStatInfo stat = stat(path);
        Assert.assertEquals(stat.getSize(), size + offset);
        Assert.assertTrue(unlink(path));
    }


    @Test
    public void testBuffOffset() {
        String path = cfileTestDir + "/f4";
        Assert.assertTrue(mkdirs(cfileTestDir));
        CFSFile cfile = openFile(path, FileStorage.O_WRONLY | FileStorage.O_CREAT);
        Assert.assertNotNull(cfile);
        byte[] buff = buffBlock2.getBytes();
        long size = 2048;
        for (int i = 0; i < size / 8; i++) {
            Assert.assertTrue(write(cfile, buff, 2, 8));
        }
        Assert.assertTrue(close(cfile));
        CFSStatInfo stat = stat(path);
        Assert.assertEquals(stat.getSize(), size);
    }

    @Test
    public void testFlags() {
        String path = cfileTestDir + "/f5";
        Assert.assertTrue(mkdirs(cfileTestDir));
        long size = 2048;
        Assert.assertTrue(createFile(path, size));
        int flags = FileStorage.O_WRONLY | FileStorage.O_CREAT;
        Assert.assertNotNull(openFile(path, flags));
        unlink(path);

        String path1 = cfileTestDir + TestHelper.getRandomUUID();
        flags = FileStorage.O_WRONLY | FileStorage.O_APPEND;
        Assert.assertNull(openFile(path1, flags));

        String path2 = cfileTestDir + TestHelper.getRandomUUID();
        flags = FileStorage.O_WRONLY | FileStorage.O_TRUNC;
        Assert.assertNull(openFile(path2, flags));

        String path3 = cfileTestDir + TestHelper.getRandomUUID();
        flags = FileStorage.O_TRUNC;
        Assert.assertNull(openFile(path3, flags));

        String path4 = cfileTestDir + TestHelper.getRandomUUID();
        flags = FileStorage.O_CREAT;
        Assert.assertNull(openFile(path4, flags));

        String path5 = cfileTestDir + TestHelper.getRandomUUID();
        flags = FileStorage.O_APPEND;
        Assert.assertNull(openFile(path5, flags));
    }

    @Test
    public void testRead() {
        String path = cfileTestDir + "/f6";
        long size = 2048;
        Assert.assertTrue(mkdirs(cfileTestDir));
        Assert.assertTrue(createFile(path, size));
        Assert.assertTrue(readFile(path, size));
        Assert.assertTrue(unlink(path));
    }

    @Test
    public void testReadBuff() {
        String path = cfileTestDir + "/f7";
        Assert.assertTrue(mkdirs(cfileTestDir));
        long size = 2048;
        Assert.assertTrue(createFile(path, size));
        Assert.assertTrue(readFile(path, size));
        CFSFile cfile = openFile(path, FileStorage.O_RDONLY);
        byte[] buff = new byte[10];
        byte[] data = buffBlock2.getBytes();
        long len = 0L;
        while (true) {
            long rsize = read(cfile, buff, 2, 8);
            if (rsize == -1) {
                break;
            }
            Assert.assertEquals(rsize, 8);
            len += rsize;
            for (int i = 2; i < 10; i++) {
                Assert.assertEquals(buff[i], data[i]);
            }
        }
        Assert.assertEquals(len, size);
        unlink(path);
    }
}