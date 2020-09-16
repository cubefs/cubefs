package io.chubao.fs.sdk;

import io.chubao.fs.sdk.exception.CFSException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;

import java.util.UUID;

public class TestHelper {
  private static final Log log = LogFactory.getLog(TestHelper.class);
  private static String sdkPath = "cfs_libsdk";
  private static String mastersKey = "cfs_masters";
  private static String volumeKey = "cfs_volume";

  public static String getSdkPath() {
    log.info("libsdk:" + System.getenv(sdkPath));
    return System.getenv(sdkPath);
  }

  public static StorageConfig getConfig() {
    StorageConfig config = new StorageConfig();
    String master = System.getenv(mastersKey);
    String vol = System.getenv(volumeKey);

    config.setMasters(master);
    config.setVolumeName(vol);
    return config;
  }

  public static String getRandomUUID() {
    return UUID.randomUUID().toString();
  }

  public static  CFSClient creatClient(String libpath) {
    try {
      CFSClient client = new CFSClient(libpath);
      client.init();
      return client;
    } catch (CFSException ex) {
      log.error(ex.getMessage());
      return null;
    }
  }

  public static  CFSClient creatClient() {
    try {
      CFSClient client = new CFSClient(getSdkPath());
      client.init();
      return client;
    } catch (CFSException ex) {
      log.error(ex.getMessage(), ex);
      return null;
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return null;
    }
  }

  public static FileStorage getStorage() {
    try {
      CFSClient client = creatClient();
      Assert.assertNotNull(client);
      StorageConfig config = getConfig();
      Assert.assertNotNull(config);
      return client.openFileStorage(config);
    } catch (CFSException ex) {
      log.error(ex.getMessage(), ex);
      return null;
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return null;
    }
  }

  public static FileStorage getStorage(StorageConfig config) {
    try {
      CFSClient client = creatClient();
      return client.openFileStorage(config);
    } catch (CFSException ex) {
      return null;
    } catch (Exception ex) {
      return null;
    }
  }
}
