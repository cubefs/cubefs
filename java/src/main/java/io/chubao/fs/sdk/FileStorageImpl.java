package io.chubao.fs.sdk;

import io.chubao.fs.sdk.exception.CFSException;
import io.chubao.fs.sdk.libsdk.CFSDriverIns;
import io.chubao.fs.sdk.libsdk.SDKStatInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FileStorageImpl implements FileStorage {
  private static final Log log = LogFactory.getLog(FileStorageImpl.class);
  private final String separator= "/";
  private CFSDriverIns driver;
  private int DEFAULT_MODE = 0644;

  public FileStorageImpl(CFSDriverIns driver) {
    this.driver = driver;
  }

  public void init() throws Exception {

  }

  @Override
  public CFSFile open(String path, int flags, int mode) throws CFSException {
    long fd = driver.open(path, flags, mode);
    if (fd < 0) {
      throw new CFSException("status code: " + fd);
    }
    return new CFSFileImpl(driver, fd);
  }

  @Override
  public boolean mkdirs(String path, int mode) throws CFSException {
    driver.mkdirs(path, mode);
    return true;
  }

  @Override
  public void truncate(String path, long newLength) throws CFSException {
    driver.truncate(path, newLength);
  }

  @Override
  public void close() throws CFSException {
    //driver.close(d);
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
    return driver.listDir(path);
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
    log.error("Not implement.");
    throw new CFSException("Not support.");
  }

  @Override
  public byte[] getXAttr(String path, String name) throws CFSException {
    log.error("Not implement.");
    throw new CFSException("Not support.");
  }

  @Override
  public List<String> listXAttr(String path) throws CFSException {
    log.error("Not implement.");
    throw new CFSException("Not support.");
  }

  @Override
  public Map<String,byte[]> getXAttrs(String path, List<String> names)
    throws CFSException {
    throw new CFSException("Not support.");
  }

  @Override
  public void removeXAttr(String path, String name) throws CFSException {
    log.error("Not implement.");
    throw new CFSException("Not support.");
  }

  @Override
  public void setOwner(String path, String username, String groupname)
      throws CFSException {
    log.error("Not implement.");
  }

  private void setAttr(String path, int mode, int uid, int gid, long mtime, long atime)
    throws CFSException {
    log.error("Not implement.");
  }

  private List<String> parsePath(String path) throws CFSException {
    ArrayList<String> res = new ArrayList<>();
    int index = 0;
    if (path.charAt(index) != '/') {
      throw new CFSException("The path must be start with /");
    }
    String[] names = path.split("/");
    int count = 0;
    for (int i=1; i<names.length-1; i++) {
      if (names[i] == "." || names[i] == "") {
        continue;
      }

      if (names[i+1] == "..") {
        if (count == 0) {
          throw new CFSException("Path is invalid.");
        }
        res.remove(count);
      } else {
        res.add(names[i]);
        count ++;
      }

    }
    return res;
  }

  @Override
  public long getBlockSize() {
    return 128 * 1024 * 1024L;
  }


  @Override
  public int getReplicaNumber() {
    return 3;
  }
}
