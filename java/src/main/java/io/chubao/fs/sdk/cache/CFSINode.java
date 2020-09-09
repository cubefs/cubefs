package io.chubao.fs.sdk.cache;

public class CFSINode {
  public static final  long DEFAUT_ROOT_IID = 1;
  private long id = 0L;
  private int mode = 0;

  public CFSINode(long iid) {
    this.id = iid;
  }

  public CFSINode(int st, long iid) {
    this.id = iid;
  }

  public CFSINode(int st,long iid, int mode) {
    this.id = iid;
    this.mode = mode;
  }

  public long getID() {
    return this.id;
  }

  public int getMode() {
    return this.mode;
  }
}

