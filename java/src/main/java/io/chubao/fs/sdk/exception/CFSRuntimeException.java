package io.chubao.fs.sdk.exception;

public class CFSRuntimeException extends RuntimeException {
  protected int code;

  public CFSRuntimeException(String msg)
  {
    super(msg);
  }

  public CFSRuntimeException(String msg, int code)
  {
    super(msg);
    this.code = code;
  }

  public CFSRuntimeException(Throwable cause)
  {
    super(cause);
  }

  public CFSRuntimeException(Throwable cause, int code)
  {
    super(cause);
    this.code = code;
  }

  public CFSRuntimeException(String msg, Throwable cause)
  {
    super(msg, cause);
  }

  public CFSRuntimeException(String msg, Throwable cause, int code)
  {
    super(msg, cause);
    this.code = code;
  }

  public int getErrorCode()
  {
    return this.code;
  }
}
