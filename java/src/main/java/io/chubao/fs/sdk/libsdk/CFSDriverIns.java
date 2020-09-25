package io.chubao.fs.sdk.libsdk;

import com.sun.jna.Pointer;
import io.chubao.fs.CfsLibrary;
import io.chubao.fs.sdk.exception.*;
import io.chubao.fs.sdk.CFSStatInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import io.chubao.fs.CfsLibrary.*;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class CFSDriverIns {
  private final static int ATTR_MODE      = 1 << 0;
  private final static int ATTR_UID       = 1 << 1;
  private final static int ATTR_GID       = 1 << 2;
  private final static int ATTR_MTIME     = 1 << 3;
  private final static int ATTR_ATIME     = 1 << 4;
  private final static int ATTR_SIZE      = 1 << 5;
  private final static int batchSize = 100;

  private static final Log log = LogFactory.getLog(CFSDriverIns.class);
  private CfsLibrary driver;
  private long clientID;

  public CFSDriverIns(CfsLibrary d, long cid) {
    this.driver = d;
    this.clientID = cid;
  }

  public int open(String path, int flags, int mode, int uid, int gid) throws CFSException {
    verifyPath(path);

    int st =driver.cfs_open(this.clientID, path, flags, mode, uid, gid);
    if (st < 0) {
      throw new CFSException("Failed to open:" + path + " status code: " + st);
    }

    return st;
  }

  public long size(int fd) throws CFSException {
    long size =driver.cfs_file_size(this.clientID, fd);
    if (size < 0) {
      throw new CFSException("Failed to get size of file:" + fd + " status code: " + size);
    }
    return size;
  }

  public void flush(int fd) throws CFSException {
    if (fd < 1) {
      throw new CFSException("Invalid argument.");
    }
    int st = driver.cfs_flush(this.clientID, fd);
    if (StatusCodes.get(st) != StatusCodes.CFS_STATUS_OK) {
      throw new CFSException("Failed to flush:" + fd + " status code:" + st);
    }
  }

  public void closeClient() {
    driver.cfs_close_client(this.clientID);
  }

  public void close(int fd) throws CFSException {
    if (fd < 1) {
      throw new CFSException("Invalid arguments.");
    }
    driver.cfs_close(this.clientID, fd);
  }

  public long write(int fd, long offset, byte[] data, long len) throws CFSException {
    if (fd < 1 || offset < 0 || len < 0) {
      throw new CFSException("Invalid arguments.");
    }
    long wsize = driver.cfs_write(this.clientID, fd, data, len, offset);
    if (wsize < 0) {
      throw new CFSException("Failed to write: " + fd + " at offset: " + offset + " the status code: " + wsize);
    }

    return wsize;
  }

  public long read(int fd, long offset, byte[] buff, int len) throws CFSException {
    if (fd < 1 || offset < 0 || len < 0) {
      throw new CFSException("Invalid arguments.");
    }
    long rsize = driver.cfs_read(this.clientID, fd, buff, len, offset);
    if (rsize == 0) {
      throw new CFSEOFException("fd:" + fd);
    }
    if (rsize < -1) {
      throw new CFSException("Failed to read fd: " + fd + " status code: " + rsize);
    }

    return rsize;
  }

  public void mkdirs(String path, int mode, int uid, int gid) throws CFSException {
    verifyPath(path);
    /*
    GoString.ByValue p = new GoString.ByValue();
    p.ptr = path;
    p.len = path.length();
     */
    int st = driver.cfs_mkdirs(this.clientID, path, mode, uid, gid);
    if (StatusCodes.get(st) != StatusCodes.CFS_STATUS_OK &&
      StatusCodes.get(st) != StatusCodes.CFS_STATUS_FILE_EXISTS) {
      throw new CFSException("Failed to mkdirs: " + path + " status code:" + st);
    }
  }

  public void rmdir(String path, boolean recursive) throws CFSException {
    log.info("rmdir:" + path + " recursive:" + recursive);
    verifyPath(path);
    /*
    GoString.ByValue p = new GoString.ByValue();
    p.ptr = path;
    p.len = path.length();

     */
    int st = driver.cfs_rmdir(this.clientID, path, recursive);
    if (StatusCodes.get(st) != StatusCodes.CFS_STATUS_OK) {
      throw new CFSException("Failed to rmdir:" + path + " status code:" + st);
    }
  }

  public void unlink(String path) throws CFSException {
    verifyPath(path);
    int st = driver.cfs_unlink(this.clientID, path);
    if (StatusCodes.get(st) != StatusCodes.CFS_STATUS_OK) {
      throw new CFSException("Failed to unlink " + path + ", the status code is " + st);
    }
  }

  public void rename(String from, String to) throws CFSException {
    verifyPath(from);
    verifyPath(to);
    int st = driver.cfs_rename(this.clientID, from, to);
    if (StatusCodes.get(st) != StatusCodes.CFS_STATUS_OK) {
      throw new CFSException("Failed to rename: " + from + " to:" + to + " status code:" + st);
    }
  }

  public void truncate(String path, long newLength) throws CFSException {
    if (newLength < 0) {
      throw new CFSException("Invalid arguments.");
    }
    verifyPath(path);

    CfsLibrary.StatInfo stat = new CfsLibrary.StatInfo();
    stat.size = newLength;
    int valid = ATTR_SIZE;
    int st = driver.cfs_setattr_by_path(this.clientID, path, stat, valid);
    if (StatusCodes.get(st) != StatusCodes.CFS_STATUS_OK) {
      throw new CFSException("Failed to truncate: " + path + " status code: " + st);
    }
  }

  public void chmod(String path, int mode) throws CFSException {
    verifyPath(path);
    CfsLibrary.StatInfo stat = new CfsLibrary.StatInfo();
    stat.mode = mode;
    int valid = ATTR_MODE;
    int st = driver.cfs_setattr_by_path(this.clientID, path, stat, valid);
    if (StatusCodes.get(st) != StatusCodes.CFS_STATUS_OK) {
      throw new CFSException("Failed to chmod: " + path + " status code: " + st);
    }
  }

  public void chown(String path, int uid, int gid) throws CFSException {
    verifyPath(path);
    CfsLibrary.StatInfo stat = new CfsLibrary.StatInfo();
    stat.uid = uid;
    stat.gid = gid;
    int valid = ATTR_GID | ATTR_UID;
    int st = driver.cfs_setattr_by_path(this.clientID, path, stat, valid);
    if (StatusCodes.get(st) != StatusCodes.CFS_STATUS_OK) {
      throw new CFSException("Failed to chown: " + path + " status code: " + st);
    }
  }

  public void setTimes(String path, long mtime, long atime) throws CFSException {
    verifyPath(path);
    CfsLibrary.StatInfo stat = new CfsLibrary.StatInfo();
    int valid = 0;
    if (mtime > 0) {
      stat.mtime = mtime;
      valid = ATTR_MTIME;
    }

    if (atime > 0) {
      stat.atime = atime;
      valid = valid | ATTR_ATIME;
    }
    int st = driver.cfs_setattr_by_path(this.clientID, path, stat, valid);
    if (StatusCodes.get(st) != StatusCodes.CFS_STATUS_OK) {
      throw new CFSException("Failed to settimes: " + path + " status code: " + st);
    }
  }

  public CfsLibrary.StatInfo getAttr(String path) throws CFSException {
    verifyPath(path);
    CfsLibrary.StatInfo.ByReference info = new CfsLibrary.StatInfo.ByReference();
    int st = driver.cfs_getattr(this.clientID, path, info);
    if (StatusCodes.get(st) == StatusCodes.CFS_STATUS_FILIE_NOT_FOUND) {
      log.info("Not found the path: " + path + " error code: " + st);
      //throw new CFSFileNotFoundException("Not found the path: " + path);
      return null;
    }
    if (StatusCodes.get(st) != StatusCodes.CFS_STATUS_OK) {
      log.error("Not stat the path: " + path + " error code: " + st);
      throw new CFSException("Failed to stat.");
    }
    log.info(info.toString());
    return info;
  }

  public int list(int fd, ArrayList<CFSStatInfo> fileStats) throws CFSException, UnsupportedEncodingException {
    CfsLibrary.Dirent dent = new CfsLibrary.Dirent();
    CfsLibrary.Dirent[] dents = (CfsLibrary.Dirent[]) dent.toArray(batchSize);

    Pointer arr = dents[0].getPointer();
    CfsLibrary.DirentArray.ByValue slice = new DirentArray.ByValue();
    slice.data = arr;
    slice.len = batchSize;
    slice.cap = batchSize;

    int count = driver.cfs_readdir(this.clientID, fd, slice, batchSize);
    if (StatusCodes.get(count) == StatusCodes.CFS_STATUS_FILIE_NOT_FOUND) {
      throw new CFSFileNotFoundException("Not found " + fd);
    }
    if (count < 0) {
      throw new CFSException("Failed to count dir:" + fd + " status code: " + count);
    }

    if (count == 0) {
      return count;
    }

    long[] iids = new long[count];
    Map<Long, String> names = new HashMap<Long, String>(count);
    for (int i=0; i<count; i++) {
      dents[i].read();
      iids[i] = dents[i].ino;
      names.put(dents[i].ino, new String(dents[i].name, 0, dents[i].nameLen, "utf-8"));
    }

    StatInfo stat = new StatInfo();
    StatInfo[] stats = (StatInfo[]) stat.toArray(count);
    Pointer statsPtr = stats[0].getPointer();
    CfsLibrary.DirentArray.ByValue statSlice = new DirentArray.ByValue();
    statSlice.data = statsPtr;
    statSlice.len = batchSize;
    statSlice.cap = batchSize;
    int num = driver.cfs_batch_get_inodes(this.clientID, fd, iids, statSlice, count);

    if (num < 0) {
      throw new CFSException("Failed to get inodes,  the fd:" + fd + " status code: " + num);
    }

    for (int i=0; i<num; i++) {
      stats[i].read();
      StatInfo in = stats[i];
      log.info(in.toString());
      try {
        CFSStatInfo info = new CFSStatInfo(
            in.mode, in.uid, in.gid, in.size,
            in.ctime, in.mtime, in.atime, names.get(in.ino));
        fileStats.add(info);

      } catch (Exception e)  {
        log.error(e.getMessage(), e);
      }
    }

    return num;
  }

  private void verifyPath(String path) throws CFSException {
    if (path == null || path.trim().length() == 0) {
      throw new CFSNullArgumentException("path is invlaid.");
    }

    if (path.startsWith("/") == false) {
      throw new  CFSInvalidArgumentException(path);
    }
  }
}
