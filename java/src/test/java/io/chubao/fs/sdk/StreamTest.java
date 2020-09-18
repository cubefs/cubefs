package io.chubao.fs.sdk;

import io.chubao.fs.sdk.stream.CFSInputStream;
import io.chubao.fs.sdk.stream.CFSOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Test;

public class StreamTest extends StorageTest {
  private final static Log log = LogFactory.getLog(StreamTest.class);
  @Test
  public void testInt() {
    String path = StorageTest.streamTestDir + "/f0";
    long size = 2048;
    int val = 32;
    Assert.assertTrue(mkdirs(StorageTest.streamTestDir));
    {
      int flags = FileStorage.O_WRONLY | FileStorage.O_CREAT;
      CFSFile cfile = openFile(path, flags);
      CFSOutputStream output = new CFSOutputStream(cfile);
      Assert.assertTrue(writeStream(output, val));
      Assert.assertTrue(closeStream(output));

    }

    {
      int flags = FileStorage.O_RDONLY;
      CFSFile cfile = openFile(path, flags);
      CFSInputStream in = new CFSInputStream(cfile);
      Assert.assertEquals(readStream(in), val);
    }

    Assert.assertTrue(unlink(path));
  }


  @Test
  public void testBytes() {
    String path = streamTestDir + "/f1";
    Assert.assertTrue(mkdirs(streamTestDir));
    byte[] data = genBuff(buffSize);
    long fileSize = 1024;
    {
      int flags = FileStorage.O_WRONLY | FileStorage.O_CREAT;
      CFSFile cfile = openFile(path, flags);
      CFSOutputStream output = new CFSOutputStream(cfile);
      for (int i=0; i<fileSize/buffSize; i++) {
        Assert.assertTrue(writeStream(output, data));
      }
      Assert.assertTrue(closeStream(output));
    }

    {
      byte[] buff = new byte[buffSize];
      int flags = FileStorage.O_RDONLY;
      CFSFile cfile = openFile(path, flags);
      CFSInputStream in = new CFSInputStream(cfile);
      int rsize = 0;
      long count = 0L;
      while (true) {
        rsize = readStream(in, buff);
        if (rsize == -1) {
          break;
        }
        Assert.assertEquals(rsize, buffSize);
        count += rsize;
        for (int i=0; i<buffSize; i++) {
          Assert.assertEquals(buff[i], data[i]);
        }
      }
      Assert.assertEquals(count, fileSize);
    }

    Assert.assertTrue(unlink(path));
  }

  @Test
  public void testBytesOffset() {
    String path = streamTestDir + "/f2";
    Assert.assertTrue(mkdirs(streamTestDir));
    byte[] data = buffBlock2.getBytes();
    long fileSize = 1024;
    {
      int flags = FileStorage.O_WRONLY | FileStorage.O_CREAT;
      CFSFile cfile = openFile(path, flags);
      CFSOutputStream output = new CFSOutputStream(cfile);
      for (int i=0; i<fileSize/buffSize; i++) {
        Assert.assertTrue(writeStream(output, data, 2, buffSize));
      }
      Assert.assertTrue(closeStream(output));
    }

    {
      byte[] buff = new byte[buffSize2];
      int flags = FileStorage.O_RDONLY;
      CFSFile cfile = openFile(path, flags);
      CFSInputStream in = new CFSInputStream(cfile);
      int rsize = 0;
      long count = 0L;
      while (true) {
        rsize = readStream(in, buff, 2, buffSize);
        if (rsize == -1) {
          break;
        }
        Assert.assertEquals(rsize, buffSize);
        count += rsize;
        for (int i=2; i<buffSize2; i++) {
          Assert.assertEquals(buff[i], data[i]);
        }
      }
      Assert.assertEquals(count, fileSize);
    }

    Assert.assertTrue(unlink(path));
  }

  @Test
  public void testSkipRead() {
    String path = streamTestDir + "/f3";
    Assert.assertTrue(mkdirs(streamTestDir));
    byte[] data = buffBlock2.getBytes();
    long fileSize = 1024;
    {
      int flags = FileStorage.O_WRONLY | FileStorage.O_CREAT;
      CFSFile cfile = openFile(path, flags);
      CFSOutputStream output = new CFSOutputStream(cfile);
      for (int i=0; i<fileSize/buffSize; i++) {
        Assert.assertTrue(writeStream(output, data, 2, buffSize));
      }
      Assert.assertTrue(closeStream(output));
    }

    {
      byte[] buff = new byte[buffSize2];
      int flags = FileStorage.O_RDONLY;
      CFSFile cfile = openFile(path, flags);
      CFSInputStream in = new CFSInputStream(cfile);
      Assert.assertEquals(skipStream(in, 1000), 1000);
      int rsize = 0;
      long count = 0L;
      while (true) {
        rsize = readStream(in, buff, 2, buffSize);
        if (rsize == -1) {
          break;
        }
        Assert.assertEquals(rsize, buffSize);
        count += rsize;
        for (int i=2; i<buffSize2; i++) {
          Assert.assertEquals(buff[i], data[i]);
        }
      }
      Assert.assertEquals(count, fileSize - 1000);
    }

    Assert.assertTrue(unlink(path));
  }
}