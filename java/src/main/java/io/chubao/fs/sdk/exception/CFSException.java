package io.chubao.fs.sdk.exception;

public class CFSException  extends Exception {
  private static final long serialVersionUID = 1L;

  protected int code;

  public CFSException(String msg)
  {
    super(msg);
  }

  public CFSException(String msg, int code)
  {
    super(msg);
    this.code = code;
  }

  public CFSException(String msg, Throwable cause)
  {
    super(msg, cause);
  }

  public CFSException(String msg, Throwable cause, int code)
  {
    super(msg, cause);
    this.code = code;
  }

  public CFSException(Throwable cause)
  {
    super(cause);
  }

  public CFSException(Throwable cause, int code)
  {
    super(cause);
    this.code = code;
  }

  public int getErrorCode()
  {
    return this.code;
  }
}