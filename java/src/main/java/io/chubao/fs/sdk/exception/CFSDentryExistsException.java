package io.chubao.fs.sdk.exception;

import static io.chubao.fs.sdk.exception.StatusCodes.CFS_STATUS_DENTRY_EXISTS;

public class CFSDentryExistsException extends CFSException {
  public CFSDentryExistsException(String msg)
  {
    super(msg, CFS_STATUS_DENTRY_EXISTS.code());
  }
}
