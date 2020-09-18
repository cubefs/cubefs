package io.chubao.fs.sdk.libsdk;
import com.sun.jna.Structure;

import java.util.ArrayList;
import java.util.List;

public class CFSOpenRes extends Structure {
  public long fd;
  public long size;
  public long pos;

  @Override
  protected List<String> getFieldOrder() {
    List<String> fields = new ArrayList<>();
    fields.add("fd");
    fields.add("size");
    fields.add("pos");
    return fields;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("fd:");
    sb.append(fd);
    sb.append(" size:");
    sb.append(size);
    sb.append(" pos:");
    sb.append(pos);
    return sb.toString();
  }

  public static class ByReference extends CFSOpenRes implements Structure.ByReference {}
  public static class ByValue extends CFSOpenRes implements Structure.ByValue {}
}
