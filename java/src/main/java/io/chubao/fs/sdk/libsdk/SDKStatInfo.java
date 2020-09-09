package io.chubao.fs.sdk.libsdk;
import com.sun.jna.Structure;

import java.util.ArrayList;
import java.util.List;

public class SDKStatInfo extends Structure {
  public long ino;
  public long size;
  public long blocks;
  public long atime;
  public long mtime;
  public long ctime;
  public int atime_nsec;
  public int mtime_nsec;
  public int ctime_nsec;
  public int mode;
  public int nlink;
  public int blk_size;
  public int uid;
  public int gid;
  public int valid;
  public byte[] name = new byte[256];

  @Override
  protected List<String> getFieldOrder() {
    List<String> fields = new ArrayList<>();
    fields.add("ino");
    fields.add("size");
    fields.add("blocks");
    fields.add("atime");
    fields.add("mtime");
    fields.add("ctime");
    fields.add("atime_nsec");
    fields.add("mtime_nsec");
    fields.add("ctime_nsec");
    fields.add("mode");
    fields.add("nlink");
    fields.add("blk_size");
    fields.add("uid");
    fields.add("gid");
    fields.add("valid");
    fields.add("name");
    return fields;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("inodeid:");
    sb.append(ino);
    sb.append(" szie:");
    sb.append(size);
    sb.append(" uid:");
    sb.append(uid);
    sb.append(" gid:");
    sb.append(gid);
    sb.append(" mode:");
    sb.append(mode);
    sb.append(" atime:");
    sb.append(atime);
    sb.append(" ctime:");
    sb.append(ctime);
    sb.append(" mtime:");
    sb.append(mtime);
    sb.append(" atime:");
    sb.append(atime);
    sb.append(" atime:");
    sb.append(atime);
    if (name != null) {
      try {
        String str = new String(name, "utf-8");
        sb.append(" name:");
        sb.append(str);
      } catch (Exception ex) {

      }
    }

    return sb.toString();
  }

  public static class ByReference extends SDKStatInfo implements Structure.ByReference {}
  public static class ByValue extends SDKStatInfo implements Structure.ByValue {}
}
