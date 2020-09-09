package io.chubao.fs.sdk.exception;


import static io.chubao.fs.sdk.exception.StatusCodes.CFS_STATUS_TIMEOUT;

public class CFSTimeoutException extends CFSException {
  public CFSTimeoutException(String msg)
  {
    super(msg, CFS_STATUS_TIMEOUT.code());
  }
}
