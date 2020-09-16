package io.chubao.fs.sdk;

import io.chubao.fs.sdk.exception.CFSNullArgumentException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class StorageConfig {
  private static final Log log = LogFactory.getLog(FileStorageImpl.class);
  public final static String CONFIG_KEY_MATSER = "masterAddr";
  public final static String CONFIG_KEY_VOLUME = "volName";
  public final static String CONFIG_KEY_FOLLOWER_READ = "followerRead";
  private String masters;
  private String volumeName;
  private String owner;
  private boolean followerRread = false;

  public StorageConfig() {}

  public void setMasters(String masters) {
    this.masters = masters;
  }

  public void setVolumeName(String volumeName) {
    this.volumeName = volumeName;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }

  public String getMasters() throws CFSNullArgumentException {
    if (masters == null) {
      throw new CFSNullArgumentException("The master is null.");
    }
    return this.masters;
  }

  public String getVolumeName() throws CFSNullArgumentException {
    if (volumeName == null) {
      throw new CFSNullArgumentException("The volume name is null.");
    }
    return this.volumeName;
  }

  public boolean getFollowerRead() {
    return this.followerRread;
  }

  public String getOwner() {
    return this.owner;
  }

  public void print() {
    log.info(CONFIG_KEY_MATSER + ":" + masters);
    log.info(CONFIG_KEY_VOLUME + ":" + volumeName);
  }
}
