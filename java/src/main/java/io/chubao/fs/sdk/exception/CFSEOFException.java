package io.chubao.fs.sdk.exception;

import static io.chubao.fs.sdk.exception.StatusCodes.CFS_STATUS_EOF;

public class CFSEOFException extends CFSException {
  public CFSEOFException(String msg)
  {
    super(msg, CFS_STATUS_EOF.code());
  }
}
