package io.chubao.fs.sdk.exception;


import static io.chubao.fs.sdk.exception.StatusCodes.CFS_STATUS_INVALID_ARGUMENT;

public class CFSInvalidArgumentException extends CFSException {
  public CFSInvalidArgumentException(String msg)
  {
    super(msg, CFS_STATUS_INVALID_ARGUMENT.code());
  }
}
