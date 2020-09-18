package io.chubao.fs.sdk.libsdk;
import com.sun.jna.Structure;

import java.util.ArrayList;
import java.util.List;

public class CFSCountDirRes extends Structure {
  public long inode;
  public int num;

  @Override
  protected List<String> getFieldOrder() {
    List<String> fields = new ArrayList<>();
    fields.add("inode");
    fields.add("num");
    return fields;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("inode:");
    sb.append(inode);
    sb.append(" num:");
    return sb.toString();
  }

  public static class ByReference extends CFSCountDirRes implements Structure.ByReference {}
  public static class ByValue extends CFSCountDirRes implements Structure.ByValue {}
}
