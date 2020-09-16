package io.chubao.fs.sdk.libsdk;

import io.chubao.fs.sdk.exception.CFSEOFException;
import io.chubao.fs.sdk.CFSStatInfo;
import io.chubao.fs.sdk.exception.CFSException;
import io.chubao.fs.sdk.exception.CFSNullArgumentException;
import io.chubao.fs.sdk.exception.StatusCodes;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CFSDriverIns {
  private final static int ATTR_MODE      = 1 << 0;
  private final static int ATTR_UID       = 1 << 1;
  private final static int ATTR_GID       = 1 << 2;
  private final static int ATTR_MTIME     = 1 << 3;
  private final static int ATTR_ATIME     = 1 << 4;
  private final static int ATTR_SIZE      = 1 << 5;

  private static final Log log = LogFactory.getLog(CFSDriverIns.class);
  private  CFSDriver driver;
  private long clientID;

  public CFSDriverIns(CFSDriver d, long cid) {
    this.driver = d;
    this.clientID = cid;
  }

  public long open(String path, int flags, int mode, int uid, int gid) throws CFSException {
    verifyPath(path);
    GoString.ByValue p = new GoString.ByValue();
    p.ptr = path;
    p.len = path.length();
    long fd =driver.cfs_open(this.clientID, p, flags, mode, uid, gid);
    if (fd < 0) {
      throw new CFSException("Failed to open:" + path + " status code: " + fd);
    }

    return fd;
  }

  public void flush(long fd) throws CFSException {
    int st = driver.cfs_flush(this.clientID, fd);
    if (StatusCodes.get(st) != StatusCodes.CFS_STATUS_OK) {
      throw new CFSException("Failed to flush:" + fd + " status code:" + st);
    }
  }

  public void closeClient() {
    driver.cfs_close_client(this.clientID);
  }

  public void close(long fd) throws CFSException {
    int st = driver.cfs_close(this.clientID, fd);
    if (StatusCodes.get(st) != StatusCodes.CFS_STATUS_OK) {
      throw new CFSException("Failed to close:" + fd + " status code:" + fd);
    }
  }

  public int write(long fd, long offset, byte[] data, int len) throws CFSException {
    if (offset < 0 || len <= 0) {
      throw new CFSException("Invalid argument.");
    }
    int wsize = driver.cfs_write(this.clientID, fd, offset, data, len);
    if (wsize < 0) {
      throw new CFSException("Failed to write " + fd + " at offset " + offset + " the status code: " + wsize);
    }

    /*
    if (wsize != len) {
      throw new CFSException("The " + wsize + " bytes written is not expected [" + len + "].");
    }
     */
    return wsize;
  }

  public int read(long fd, long offset, byte[] buff, int len) throws CFSException {
    int rsize = driver.cfs_read(this.clientID, fd, offset, buff, len);
    /*
    byte[] bf = new byte[64];
    GoBuffer.ByReference gobuff = new GoBuffer.ByReference(bf);
    //GoBuffer gobuff = new GoBuffer(bf);
    Pointer pointer = gobuff.getPointer();
    System.out.println("pointer:" + pointer);
    //int rsize = driver.cfs_read(this.clientID, fd, offset, pointer, 64);
    int rsize = driver.cfs_read(this.clientID, fd, offset, bf, 64);
    try {
      String data =  new String(bf, "utf8");
      log.info("data:" + data);
      System.out.println("buff:" + Arrays.toString(bf));
    } catch (Exception e) {

    }
    if (pointer != null)
      throw new RuntimeException("fd:" + fd);
     */
    if (rsize == -5) {
      throw new CFSEOFException("fd:" + fd);
    }
    if (rsize < -1) {
      throw new CFSException("Failed to read fd: " + fd + " status code: " + rsize);
    }

    return rsize;
  }

  public void mkdirs(String path, int mode, int uid, int gid) throws CFSException {
    verifyPath(path);
    GoString.ByValue p = new GoString.ByValue();
    p.ptr = path;
    p.len = path.length();
    int st = driver.cfs_mkdirs(this.clientID, p, mode, uid, gid);
    if (StatusCodes.get(st) != StatusCodes.CFS_STATUS_OK) {
      throw new CFSException("Failed to mkdirs: " + path + " status code:" + st);
    }
  }

  public void rmdir(String path, boolean recursive) throws CFSException {
    verifyPath(path);
    GoString.ByValue p = new GoString.ByValue();
    p.ptr = path;
    p.len = path.length();
    int st = driver.cfs_rmdir(this.clientID, p, recursive);
    if (StatusCodes.get(st) != StatusCodes.CFS_STATUS_OK) {
      throw new CFSException("Failed to rmdir:" + path + " status code:" + st);
    }
  }

  public void unlink(String path) throws CFSException {
    verifyPath(path);
    GoString.ByValue p = new GoString.ByValue();
    p.ptr = path;
    p.len = path.length();
    int st = driver.cfs_unlink(this.clientID, p);
    if (StatusCodes.get(st) != StatusCodes.CFS_STATUS_OK) {
      throw new CFSException("Failed to unlink " + path + ", the status code is " + st);
    }
  }

  public void rename(String from, String to) throws CFSException {
    verifyPath(from);
    verifyPath(to);
    GoString.ByValue src = new GoString.ByValue();
    src.ptr = from;
    src.len = from.length();

    GoString.ByValue target = new GoString.ByValue();
    target.ptr = to;
    target.len = to.length();
    int st = driver.cfs_rename(this.clientID, src, target);
    if (StatusCodes.get(st) != StatusCodes.CFS_STATUS_OK) {
      throw new CFSException("Failed to rename: " + from + " to:" + to + " status code:" + st);
    }
  }

  public void truncate(String path, long newLength) throws CFSException {
    verifyPath(path);
    GoString.ByValue p = new GoString.ByValue();
    p.ptr = path;
    p.len = path.length();

    SDKStatInfo.ByReference stat = new SDKStatInfo.ByReference();
    stat.size = newLength;
    stat.valid = ATTR_SIZE;
    int st = driver.cfs_setattr_by_path(this.clientID, p, stat);
    if (StatusCodes.get(st) != StatusCodes.CFS_STATUS_OK) {
      throw new CFSException("Failed to truncate: " + path + " status code: " + st);
    }

  }

  public void chmod(String path, int mode) throws CFSException {
    verifyPath(path);
    GoString.ByValue p = new GoString.ByValue();
    p.ptr = path;
    p.len = path.length();

    SDKStatInfo.ByReference stat = new SDKStatInfo.ByReference();
    stat.mode = mode;
    stat.valid = ATTR_MODE;
    int st = driver.cfs_setattr_by_path(this.clientID, p, stat);
    if (StatusCodes.get(st) != StatusCodes.CFS_STATUS_OK) {
      throw new CFSException("Failed to chmod: " + path + " status code: " + st);
    }
  }

  public void chown(String path, int uid, int gid) throws CFSException {
    verifyPath(path);
    GoString.ByValue p = new GoString.ByValue();
    p.ptr = path;
    p.len = path.length();

    SDKStatInfo.ByReference stat = new SDKStatInfo.ByReference();
    stat.uid = uid;
    stat.gid = gid;
    stat.valid = ATTR_GID | ATTR_UID;
    int st = driver.cfs_setattr_by_path(this.clientID, p, stat);
    if (StatusCodes.get(st) != StatusCodes.CFS_STATUS_OK) {
      throw new CFSException("Failed to chown: " + path + " status code: " + st);
    }
  }

  public void setTimes(String path, long mtime, long atime) throws CFSException {
    verifyPath(path);
    GoString.ByValue p = new GoString.ByValue();
    p.ptr = path;
    p.len = path.length();

    SDKStatInfo.ByReference stat = new SDKStatInfo.ByReference();
    if (mtime > 0) {
      stat.mtime = mtime;
      stat.valid = ATTR_MTIME;
    }

    if (atime > 0) {
      stat.atime = atime;
      stat.valid = stat.valid | ATTR_ATIME;
    }
    int st = driver.cfs_setattr_by_path(this.clientID, p, stat);
    if (StatusCodes.get(st) != StatusCodes.CFS_STATUS_OK) {
      throw new CFSException("Failed to settimes: " + path + " status code: " + st);
    }
  }

  public SDKStatInfo getAttr(String path) throws CFSException {
    verifyPath(path);
    GoString.ByValue p = new GoString.ByValue();
    p.ptr = path;
    p.len = path.length();
    SDKStatInfo.ByReference info = new SDKStatInfo.ByReference();
    //SDKStatInfo info = new SDKStatInfo();
    int st = driver.cfs_getattr(this.clientID, p, info);
    log.info(info.toString());
    if (StatusCodes.get(st) == StatusCodes.CFS_STATUS_FILIE_NOT_FOUND) {
      log.error("Not fount the path: " + path + " error code: " + st);
      //throw new CFSFileNotFoundException("Not fount the path: " + path);
      return null;
    }
    if (StatusCodes.get(st) != StatusCodes.CFS_STATUS_OK) {
      log.error("Not stat the path: " + path + " error code: " + st);
      throw new CFSException("Failed to stat.");
    }

    return info;
  }

  public CFSStatInfo[] listDir(String path) throws CFSException {
    verifyPath(path);
    log.info("path:" + path.toString() + " len:" + path.length());
    GoString.ByValue p = new GoString.ByValue();
    p.ptr = path;
    p.len = path.length();

    SDKStatInfo info = new SDKStatInfo();
    SDKStatInfo[] infos = (SDKStatInfo[])info.toArray(100);

    int count = driver.cfs_listattr(this.clientID, p, infos, 100);
    if (count < 0) {
      throw new CFSException("Failed to list dir:" + path + " status code: " + count);
    }

    CFSStatInfo[] fileInfos = new CFSStatInfo[count];
    for (int i=0; i<count; i++) {
      SDKStatInfo in = infos[i];
      log.info(in.toString());
      try {
        fileInfos[i] = new CFSStatInfo(
            in.mode, in.uid, in.gid, in.size,
            in.ctime, in.mtime, in.atime, new String(in.name, "utf-8"));

      } catch (Exception e)  {
        log.error(e);
      }
    }

    return fileInfos;
  }

  private void verifyPath(String path) throws CFSNullArgumentException {
    if (path == null || path.trim().length() == 0) {
      throw new CFSNullArgumentException("path is invlaid.");
    }
  }
}
