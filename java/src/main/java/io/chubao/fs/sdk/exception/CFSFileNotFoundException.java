package io.chubao.fs.sdk.exception;

import static io.chubao.fs.sdk.exception.StatusCodes.CFS_STATUS_DENTRY_NOT_FOUND;

public class CFSFileNotFoundException extends CFSException {
  public CFSFileNotFoundException(String msg)
  {
    super(msg, CFS_STATUS_DENTRY_NOT_FOUND.code());
  }
}
