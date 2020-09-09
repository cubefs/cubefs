package io.chubao.fs.sdk;

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
  private int cacheSize;

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

  public void setCacheSize(int size) {
    this.cacheSize = size;
  }

  public String getMasters() {
    return this.masters;
  }

  public String getVolumeName() {
    return this.volumeName;
  }


  public int getCacheSize() {
    return this.cacheSize;
  }

  public boolean getFollowerRead() {
    return this.followerRread;
  }

  public void print() {
    log.info(CONFIG_KEY_MATSER + ":" + masters);
    log.info(CONFIG_KEY_VOLUME + ":" + volumeName);
  }
}
