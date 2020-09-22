package io.chubao.fs.sdk;

public class CFSStatInfo {
  private long inodeId;
  private int mode;
  private int uid;
  private int gid;
  private long size;
  private long ctime;
  private long mtime;
  private long atime;
  private String name;

  public enum Type {
    REG,
    DIR,
    LINK,
    UNKNOWN
  }

  public CFSStatInfo(int mode, int uid, int gid, long size, long ctime, long mtime, long atime) {
    this.mode = mode;
    this.uid = uid;
    this.gid = gid;
    this.size = size;
    this.ctime = ctime;
    this.mtime= mtime;
    this.atime = atime;
  }

  public CFSStatInfo(int mode, int uid, int gid, long size, long ctime, long mtime, long atime, String name) {
    this.mode = mode;
    this.uid = uid;
    this.gid = gid;
    this.size = size;
    this.ctime = ctime;
    this.mtime= mtime;
    this.atime = atime;
    this.name = name;
  }

  public int getMode() {
    return this.mode;
  }

  public int getUid() {
    return this.uid;
  }

  public int getGid() {
    return this.gid;
  }

  public long getCtime() {
    return this.ctime;
  }

  public Type getType () {
    if ((mode & FileStorage.S_IFDIR) == FileStorage.S_IFDIR) {
      return Type.DIR;
    }

    if ((mode & FileStorage.S_IFREG) == FileStorage.S_IFREG) {
      return Type.REG;
    }

    if ((mode & FileStorage.S_IFLNK) == FileStorage.S_IFLNK) {
      return Type.LINK;
    }

    return Type.UNKNOWN;
  }

  public long getAtime() {
    return this.atime;
  }

  public long getMtime() {
    return this.mtime;
  }

  public long getSize() {
    return this.size;
  }

  public String getName() {
    return this.name;
  }
}