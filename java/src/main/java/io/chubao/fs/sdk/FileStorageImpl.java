package io.chubao.fs.sdk;

import io.chubao.fs.sdk.exception.CFSException;
import io.chubao.fs.sdk.libsdk.CFSDriverIns;
import io.chubao.fs.sdk.libsdk.CFSOpenRes;
import io.chubao.fs.sdk.libsdk.SDKStatInfo;
import io.chubao.fs.sdk.util.CFSOwnerHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;
import java.util.Map;

public class FileStorageImpl implements FileStorage {
  private static final Log log = LogFactory.getLog(FileStorageImpl.class);
  private CFSDriverIns driver;
  private CFSOwnerHelper owner;
  private long defaultBlockSize = 128 * 1024 * 1024;

  public FileStorageImpl(CFSDriverIns driver) {
    this.driver = driver;
  }

  public void init() throws Exception {
    owner = new CFSOwnerHelper();
    owner.init();
  }

  @Override
  public CFSFile open(String path, int flags, int mode, int uid, int gid) throws CFSException {
    CFSOpenRes.ByReference res = driver.open(path, flags, mode, uid, gid);
    return new CFSFileImpl(driver, res.fd, res.size, res.pos);
  }

  @Override
  public boolean mkdirs(String path, int mode, int uid, int gid) throws CFSException {
    driver.mkdirs(path, mode, uid, gid);
    return true;
  }

  @Override
  public void truncate(String path, long newLength) throws CFSException {
    driver.truncate(path, newLength);
  }

  @Override
  public void close() throws CFSException {
    driver.closeClient();
  }

  @Override
  public void rmdir(String path, boolean recursive) throws CFSException {
    driver.rmdir(path, recursive);
  }

  @Override
  public void unlink(String path) throws CFSException {
    driver.unlink(path);
  }

  @Override
  public void rename(String src, String dst) throws CFSException {
    driver.rename(src, dst);
  }

  @Override
  public CFSStatInfo[] listFileStatus(String path) throws CFSException {
    return driver.list(path);
  }

  @Override
  public CFSStatInfo stat(String path) throws CFSException {
    SDKStatInfo info = driver.getAttr(path);
    if (info == null) {
      return null;
    }
    return new CFSStatInfo(
        info.mode, info.uid, info.gid, info.size,
        info.ctime, info.mtime, info.atime);
  }

  @Override
  public void setXAttr(String path, String name, byte[] value) throws CFSException {
    throw new CFSException("Not implement setXAttr.");
  }

  @Override
  public byte[] getXAttr(String path, String name) throws CFSException {
    throw new CFSException("Not implement getXAttr.");
  }

  @Override
  public List<String> listXAttr(String path) throws CFSException {
    throw new CFSException("Not implement listXAttr.");
  }

  @Override
  public Map<String,byte[]> getXAttrs(String path, List<String> names)
    throws CFSException {
    throw new CFSException("Not implement getXAttrs.");
  }

  @Override
  public void removeXAttr(String path, String name) throws CFSException {
    log.error("Not implement.");
    throw new CFSException("Not implement removeXAttr.");
  }

  @Override
  public void chmod(String path, int mode) throws CFSException {
    driver.chmod(path, mode);
  }

  @Override
  public void chown(String path, int uid, int gid) throws CFSException {
    driver.chown(path, uid, gid);
  }

  @Override
  public void chown(String path, String user, String group) throws CFSException {
    driver.chown(path, owner.getUid(user), owner.getGid(group));
  }

  @Override
  public void setTimes(String path, long mtime, long atime) throws CFSException {
    driver.setTimes(path, mtime, atime);
  }

  @Override
  public int getUid(String username) throws CFSException {
    return owner.getUid(username);
  }

  @Override
  public int getGid(String group) throws CFSException {
    return owner.getGid(group);
  }

  @Override
  public int getGidByUser(String user) throws CFSException {
    return owner.getGidByUser(user);
  }

  @Override
  public String getUser(int uid) throws CFSException {
    return owner.getUser(uid);
  }

  @Override
  public String getGroup(int gid) throws CFSException {
    return owner.getGroup(gid);
  }

  private void setAttr(String path, int mode, int uid, int gid, long mtime, long atime)
    throws CFSException {
    throw new CFSException("Not implement setAttr.");
  }

  @Override
  public long getBlockSize() {
    return defaultBlockSize;
  }


  @Override
  public int getReplicaNumber() {
    return 3;
  }
}
