package io.chubao.fs.sdk;

import io.chubao.fs.CfsLibrary;
import io.chubao.fs.sdk.exception.CFSException;
import io.chubao.fs.sdk.exception.CFSNullArgumentException;
import io.chubao.fs.sdk.exception.StatusCodes;
import io.chubao.fs.sdk.libsdk.CFSDriverIns;
import com.sun.jna.Native;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

public class CFSClient {
  private static final Log log = LogFactory.getLog(CFSClient.class);
  private static AtomicLong clientID = new AtomicLong(0L);
  private static FileStorageImpl storage = null;
  private CfsLibrary driver;
  private String sdkLibPath;

  public CFSClient(String libpath) {
    this.sdkLibPath = libpath;
  }

  public void init() throws CFSException {
    if (sdkLibPath == null) {
      throw new CFSNullArgumentException("Please specify the libsdk.so path.");
    }
    File file = new File(sdkLibPath);
    if (file.exists() == false) {
      throw new CFSNullArgumentException("Not found the libsdk.so: " + sdkLibPath);
    }
    driver = Native.load(sdkLibPath, CfsLibrary.class);
  }

  public FileStorage openFileStorage(StorageConfig config) throws CFSException {
    /*
    if (clientID.get() > 0 && storage != null) {
      return storage;
    }
     */
    long cid = driver.cfs_new_client();
    if (cid < 0) {
      throw new CFSException("Failed to new a client.");
    }
    clientID.set(cid);

    /*
    GoString.ByValue master = new GoString.ByValue();
    master.ptr = StorageConfig.CONFIG_KEY_MATSER;
    master.len = StorageConfig.CONFIG_KEY_MATSER.length();
    GoString.ByValue masterVal = new GoString.ByValue();
    masterVal.ptr = config.getMasters();
    masterVal.len = config.getMasters().length();
    driver.cfs_set_client(cid, master, masterVal);
    GoString.ByValue volName = new GoString.ByValue();
    volName.ptr = StorageConfig.CONFIG_KEY_VOLUME;
    volName.len = StorageConfig.CONFIG_KEY_VOLUME.length();
    GoString.ByValue volNameVal = new GoString.ByValue();
    volNameVal.ptr = config.getVolumeName();
    volNameVal.len = config.getVolumeName().length();
    driver.cfs_set_client(cid, volName, volNameVal);
    driver.cfs_set_client(cid, volName, volNameVal);

     */
    driver.cfs_set_client(cid, StorageConfig.CONFIG_KEY_MATSER, config.getMasters());
    driver.cfs_set_client(cid, StorageConfig.CONFIG_KEY_VOLUME, config.getVolumeName());
    driver.cfs_set_client(cid, StorageConfig.CONFIG_KEY_LOG_DIR, config.getLogDir());
    driver.cfs_set_client(cid, StorageConfig.CONFIG_KEY_LOG_LEVEL, config.getLogLevel());


    int st = driver.cfs_start_client(cid);
    if (StatusCodes.get(st) != StatusCodes.CFS_STATUS_OK) {
      throw new CFSException("Failed to start the client: " + cid + " status code: " + st);
    }

    st = driver.cfs_chdir(cid, "/");
    if (StatusCodes.get(st) != StatusCodes.CFS_STATUS_OK) {
      throw new CFSException("Failed to chdir for client: " + cid + " status code: " + st);
    }

    try {
      CFSDriverIns ins = new CFSDriverIns(driver, cid);
      //FileStorageImpl storage = new FileStorageImpl(ins);
      storage = new FileStorageImpl(ins);
      storage.init();
      log.info("Succ to open FileStorage, client id:" + cid);
      return storage;
    } catch (Exception ex) {
      log.error(ex.getMessage(), ex);
      throw new RuntimeException(ex);
    }
  }
}
