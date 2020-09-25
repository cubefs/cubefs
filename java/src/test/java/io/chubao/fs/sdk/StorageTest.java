package io.chubao.fs.sdk;

import io.chubao.fs.sdk.exception.CFSEOFException;
import io.chubao.fs.sdk.exception.CFSException;
import io.chubao.fs.sdk.stream.CFSInputStream;
import io.chubao.fs.sdk.stream.CFSOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.*;

import java.io.IOException;

public class StorageTest {
  private final static Log log = LogFactory.getLog(StorageTest.class);

  protected static FileStorage storage;
  protected static final String mkdirsTestDir = "/mkdirstest/";
  protected static final String rmdirTestDir = "/rmdirtest/";
  protected static final String listTestDir = "/listtest/";
  protected static final String unlinkTestDir = "/unlinktest/";
  protected static final String renameTestDir = "/renametest/";
  protected static final String createTestDir = "/createtest/";
  protected static final String cfileTestDir = "/cfiletest/";
  protected static final String streamTestDir = "/streamtest/";
  protected static final String appendTestDir = "/appendtest/";
  protected static int DEFAULT_UID;
  protected static int DEFAULT_GID;
  protected static int DEFAULT_MODE = 0644;
  protected static final String buffBlock = "01234567";
  protected static final String buffBlock2 = "0001234567";
  protected static final int buffSize = 8;
  protected static final int buffSize2 = 10;

  @BeforeClass
  public static void beforeClass() {
    try  {
      storage = TestHelper.getStorage();
      DEFAULT_GID = storage.getGid("root");
      DEFAULT_UID = storage.getUid("root");
    } catch (CFSException ex) {
      Assert.assertFalse(false);
    }
  }

  @AfterClass
  public static void afterClass() {
    try  {
      storage.close();
    } catch (CFSException ex) {
      Assert.assertFalse(false);
    }
  }

  @After
  public void teardown() {
    rmdir(mkdirsTestDir, true);
    rmdir(rmdirTestDir, true);
    rmdir(listTestDir, true);
    rmdir(unlinkTestDir, true);
    rmdir(renameTestDir, true);
    rmdir(createTestDir, true);
    rmdir(cfileTestDir, true);
    rmdir(streamTestDir, true);
    rmdir(appendTestDir, true);
  }

  protected boolean mkdirs(String path) {
    try {
      storage.mkdirs(path, DEFAULT_MODE, DEFAULT_UID, DEFAULT_GID);
      CFSStatInfo stat = storage.stat(path);
      Assert.assertNotNull(stat);
      Assert.assertEquals(stat.getType(), CFSStatInfo.Type.DIR);
      return true;
    } catch (CFSException e) {
      log.error(e.getMessage(), e);
      return false;
    }
  }

  protected CFSStatInfo stat(String path) {
    try {
      CFSStatInfo stat = storage.stat(path);
      return stat;
    } catch (CFSException e) {
      log.error(e.getMessage(), e);
      return null;
    }
  }

  protected boolean rmdir(String path, boolean recursive) {
    try {
      storage.rmdir(path, recursive);
      return true;
    } catch (CFSException ex) {
      log.error(ex.getMessage(), ex);
      return false;
    }
  }

  protected boolean rename(String from, String to) {
    try {
      storage.rename(from, to);
      return true;
    } catch (CFSException ex) {
      log.error(ex.getMessage(), ex);
      return false;
    }

  }

  protected boolean unlink(String path) {
    try {
      storage.unlink(path);
      return true;
    } catch (CFSException ex) {
      log.error(ex.getMessage(), ex);
      return false;
    }
  }

  protected void checkFileStat(CFSStatInfo stat) {
    Assert.assertEquals(stat.getType(), CFSStatInfo.Type.REG);
    Assert.assertEquals(stat.getUid(), DEFAULT_UID);
    Assert.assertEquals(stat.getGid(), DEFAULT_GID);
    //Assert.assertEquals(stat.getMode(), DEFAULT_MODE);
  }

  protected void checkDirStat(CFSStatInfo stat) {
    Assert.assertEquals(stat.getType(), CFSStatInfo.Type.DIR);
    Assert.assertEquals(stat.getUid(), DEFAULT_UID);
    Assert.assertEquals(stat.getGid(), DEFAULT_GID);
    //Assert.assertEquals(stat.getMode(), DEFAULT_MODE);
  }

  protected CFSFile openFile(String path, int flags) {
    try {
      CFSFile cfile = storage.open(path, flags, DEFAULT_MODE, DEFAULT_UID, DEFAULT_GID);
      return cfile;
    } catch (CFSException e) {
      log.error(e.getMessage(), e);
      return null;
    }
  }

  protected boolean createFile(String path, long size) {
    try {
      int flags = FileStorage.O_WRONLY | FileStorage.O_CREAT;
      CFSFile cfile = storage.open(path, flags, DEFAULT_MODE, DEFAULT_UID, DEFAULT_GID);
      if (size == 0) {
        cfile.close();
        return true;
      }
      byte[] buff = genBuff(buffSize);
      for (int i=0; i<size/buffSize; i++) {
        cfile.write(buff, 0, buffSize);
      }
      cfile.close();
      Assert.assertEquals(cfile.getFileSize(), size);

      CFSStatInfo stat = stat(path);
      Assert.assertEquals(stat.getSize(), size);
      return true;
    } catch (CFSException e) {
      log.error(e.getMessage(), e);
      return false;
    }
  }

  protected boolean appendFile(String path, long size) {
    try {
      int flags = FileStorage.O_WRONLY | FileStorage.O_APPEND;
      CFSFile cfile = storage.open(path, flags, DEFAULT_MODE, DEFAULT_UID, DEFAULT_GID);
      if (size == 0) {
        cfile.close();
        return true;
      }
      CFSStatInfo stat = stat(path);
      long newsize = stat.getSize() + size;
      byte[] buff = genBuff(buffSize);
      for (int i=0; i<size/buffSize; i++) {
        cfile.write(buff, 0, buffSize);
      }
      Assert.assertEquals(cfile.getFileSize(), newsize);
      cfile.close();

      stat = stat(path);
      Assert.assertEquals(stat.getSize(), newsize);
      return true;
    } catch (CFSException e) {
      log.error(e.getMessage(), e);
      return false;
    }
  }

  protected boolean truncateFile(String path, long size) {
    try {
      int flags = FileStorage.O_WRONLY | FileStorage.O_TRUNC;
      CFSFile cfile = storage.open(path, flags, DEFAULT_MODE, DEFAULT_UID, DEFAULT_GID);
      if (size == 0) {
        cfile.close();
        return true;
      }
      byte[] buff = genBuff(buffSize);
      for (int i=0; i<size/buffSize; i++) {
        cfile.write(buff, 0, buffSize);
      }
      Assert.assertEquals(cfile.getFileSize(), size);
      cfile.close();

      CFSStatInfo stat = stat(path);
      Assert.assertEquals(stat.getSize(), size);
      return true;
    } catch (CFSException e) {
      log.error(e.getMessage(), e);
      return false;
    }
  }

  public boolean seek(CFSFile cfile, long offset) {
    try {
      cfile.seek(offset);
      return true;
    } catch (CFSException ex) {
      log.error(ex.getMessage(), ex);
      return false;
    }
  }

  protected boolean readFile(String path, long size) {
    long len = 0L;
    CFSFile cfile = null;
    try {
      cfile = storage.open(path, FileStorage.O_RDONLY, DEFAULT_MODE, DEFAULT_UID, DEFAULT_GID);
      byte[] buff = new byte[buffSize];
      byte[] data = buffBlock.getBytes();
      while (true) {
        long rsize = cfile.read(buff, 0, buffSize);
        if (rsize == -1) {
          break;
        }

        Assert.assertEquals(rsize, buffSize);
        for (int i=0; i<buffSize; i++) {
          Assert.assertEquals(buff[i], data[i]);
        }
        len += rsize;
      }
      Assert.assertEquals(len, size);
      cfile.close();
      return true;
    } catch (CFSEOFException e) {
      Assert.assertEquals(len, size);
      try {
        cfile.close();
      } catch (CFSException x) {

      }
      return true;
    } catch (CFSException e) {
      log.error(e.getMessage(), e);
      return false;
    }
  }

  protected CFSFile openFileWithCreate(String path) {
    try {
      int flags = FileStorage.O_WRONLY | FileStorage.O_CREAT;
      CFSFile cfile = storage.open(path, flags, DEFAULT_MODE, DEFAULT_UID, DEFAULT_GID);
      return cfile;
    } catch (CFSException e) {
      log.error(e.getMessage(), e);
      return null;
    }
  }

  protected boolean appendFile(String path) {
    try {
      int flags = FileStorage.O_WRONLY | FileStorage.O_APPEND;
      CFSFile cfile = storage.open(path, flags, DEFAULT_MODE, DEFAULT_UID, DEFAULT_GID);
      cfile.close();
      return true;
    } catch (CFSException e) {
      log.error(e.getMessage(), e);
      return false;
    }
  }

  protected CFSStatInfo[] listStats(String path) {
    try {
      CFSStatInfo[] stats = storage.list(path);
      if (stats == null || stats.length == 0) {
        return null;
      } else {
        return stats;
      }
    } catch (CFSException ex) {
      log.error(ex.getMessage(), ex);
      return null;
    }
  }

  protected boolean write(CFSFile cfile, byte[] buff, int buffOffset, int len) {
    try {
      cfile.write(buff, buffOffset, len);
      return true;
    } catch (CFSException ex) {
      log.error(ex.getMessage(), ex);
      return false;
    }
  }

  protected long read(CFSFile cfile, byte[] buff, int buffOffset, int len) {
    try {
      return cfile.read(buff, buffOffset, len);
    } catch (CFSEOFException x) {
      return -1;
    } catch (CFSException ex) {
      log.error(ex.getMessage(), ex);
      return -2;
    }
  }

  protected boolean close(CFSFile cfile) {
    try {
      cfile.close();
      return true;
    } catch (CFSException ex) {
      log.error(ex.getMessage(), ex);
      return false;
    }
  }

  protected byte[] genBuff(int size) {
    StringBuilder sb = new StringBuilder();
    for (int i=0; i<size/buffSize; i++) {
      sb.append(buffBlock);
    }
    return sb.toString().getBytes();
  }

  protected boolean writeStream(CFSOutputStream out, int val) {
    try {
      out.write(val);
      return true;
    }  catch (IOException ex) {
      log.error(ex.getMessage(), ex);
      return false;
    }
  }

  protected boolean writeStream(CFSOutputStream out, byte[] buff) {
    try {
      out.write(buff);
      return true;
    }  catch (IOException ex) {
      log.error(ex.getMessage(), ex);
      return false;
    }
  }

  protected boolean writeStream(CFSOutputStream out, byte[] buff, int off, int len) {
    try {
      out.write(buff, off, len);
      return true;
    }  catch (IOException ex) {
      log.error(ex.getMessage(), ex);
      return false;
    }
  }

  protected int readStream(CFSInputStream in) {
    try {
      return in.read();
    }  catch (IOException ex) {
      log.error(ex.getMessage(), ex);
      return -1;
    }
  }

  protected int readStream(CFSInputStream in, byte[] buff) {
    try {
      return in.read(buff);
    }  catch (IOException ex) {
      log.error(ex.getMessage(), ex);
      return -2;
    }
  }

  protected int readStream(CFSInputStream in, byte[] buff, int off, int len) {
    try {
      return in.read(buff, off, len);
    }  catch (IOException ex) {
      log.error(ex.getMessage(), ex);
      return -2;
    }
  }

  protected boolean closeStream(CFSOutputStream out) {
    try {
      out.close();
      return true;
    }  catch (IOException ex) {
      log.error(ex.getMessage(), ex);
      return false;
    }
  }

  protected long skipStream(CFSInputStream in, long n) {
    try {
      return in.skip(n);
    }  catch (IOException ex) {
      log.error(ex.getMessage(), ex);
      return -1;
    }
  }
}
