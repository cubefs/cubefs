package io.chubao.fs.sdk;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Test;

public class CFSFileTest extends StorageTest {
  private final static Log log = LogFactory.getLog(CFSFileTest.class);
  @Test
  public void testCreate() {
    String path = createTestDir + "/f0";
    Assert.assertTrue(mkdirs(appendTestDir));
    long size = 2048;
    Assert.assertTrue(createFile(path, size));
    Assert.assertTrue(unlink(path));
  }

  @Test
  public void testAppend() {
    String path = appendTestDir + "/f0";
    Assert.assertTrue(mkdirs(appendTestDir));
    long size = 2048;
    Assert.assertTrue(createFile(path, size));
    size = 4096;
    Assert.assertTrue(appendFile(path, size));
    Assert.assertTrue(unlink(path));
  }

  @Test
  public void testTruncate() {
    String path = overwriteTestDir + "/f0";
    Assert.assertTrue(mkdirs(overwriteTestDir));
    long size = 2048;
    Assert.assertTrue(createFile(path, size));
    size = 4096;
    Assert.assertTrue(truncateFile(path, size));
    Assert.assertTrue(unlink(path));
  }

  @Test
  public void testSeek() {
    String path = overwriteTestDir + "/f0";
    long size = 2048;
    Assert.assertTrue(mkdirs(overwriteTestDir));
    Assert.assertTrue(createFile(path, size));
    CFSFile cfile = openFile(path, FileStorage.O_WRONLY);
    long offset = 1;
    Assert.assertTrue(seek(cfile, size));

    byte[] buff = genBuff(buffSize);
    for (int i=0; i<size/buffSize; i++) {
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
    String path = createTestDir + "/f0";
    Assert.assertTrue(mkdirs(createTestDir));
    CFSFile cfile = openFile(path, FileStorage.O_WRONLY);
    byte[] buff = buffBlock2.getBytes();
    long size = 2048;
    for (int i=0; i<size/8; i++) {
      Assert.assertTrue(write(cfile, buff, 2, 8));
    }
    Assert.assertTrue(close(cfile));
    CFSStatInfo stat = stat(path);
    Assert.assertEquals(stat.getSize(), size);
  }

  @Test
  public void testFlags() {
    String path = createTestDir + "/f0";
    Assert.assertTrue(mkdirs(createTestDir));
    long size = 2048;
    Assert.assertTrue(createFile(path, size));
    int flags = FileStorage.O_WRONLY | FileStorage.O_CREAT;
    Assert.assertNull(openFile(path, flags));
    unlink(path);

    String path1 = createTestDir + TestHelper.getRandomUUID();
    flags = FileStorage.O_WRONLY | FileStorage.O_APPEND;
    Assert.assertNull(openFile(path1, flags));

    String path2 = createTestDir + TestHelper.getRandomUUID();
    flags = FileStorage.O_WRONLY | FileStorage.O_TRUNC;
    Assert.assertNull(openFile(path2, flags));

    String path3 = createTestDir + TestHelper.getRandomUUID();
    flags = FileStorage.O_TRUNC;
    Assert.assertNull(openFile(path3, flags));

    String path4 = createTestDir + TestHelper.getRandomUUID();
    flags = FileStorage.O_CREAT;
    Assert.assertNull(openFile(path4, flags));

    String path5 = createTestDir + TestHelper.getRandomUUID();
    flags = FileStorage.O_APPEND;
    Assert.assertNull(openFile(path5, flags));
  }

  @Test
  public void testRead() {
    String path = readTestDir + "/f0";
    long size = 2048;
    Assert.assertTrue(mkdirs(readTestDir));
    Assert.assertTrue(createFile(path, size));
    Assert.assertTrue(readFile(path, size));
    Assert.assertTrue(unlink(path));
  }

  @Test
  public void testReadBuff() {
    String path = readTestDir + "/f0";
    Assert.assertTrue(mkdirs(readTestDir));
    long size = 2048;
    Assert.assertTrue(createFile(path, size));
    Assert.assertTrue(readFile(path, size));
    Assert.assertTrue(unlink(path));
    CFSFile cfile = openFile(path, FileStorage.O_RDONLY);
    byte[] buff = new byte[10];
    byte[] data = buffBlock2.getBytes();
    long len = 0L;
    while (true) {
      int rsize = read(cfile, buff, 2, 8);
      if (rsize == -1) {
        break;
      }
      Assert.assertEquals(rsize, -2);
      len += rsize;
      for (int i=2; i<10; i++) {
        Assert.assertEquals(buff[i], data[2]);
      }
    }
    Assert.assertEquals(len, size);
    unlink(path);
  }
}