package io.chubao.fs.sdk.exception;

import org.apache.commons.lang.NullArgumentException;

public enum StatusCodes
{
  CFS_STATUS_OK(0, "Ok"),
  CFS_STATUS_ERROR(1, "Error"),
  CFS_STATUS_TIMEOUT(2, "Timeout"),
  CFS_STATUS_INVALID_ARGUMENT(3, "Invalid argument."),
  CFS_STATUS_FILE_OPEN_FAILED(4, "File open failed"),
  CFS_STATUS_FILE_EXISTS(-17, "File exists"),
  CFS_STATUS_IO_ERROR(6, "IO error"),
  CFS_STATUS_DENTRY_EXISTS(7, "Dentry is exist."),
  CFS_STATUS_DENTRY_NOT_FOUND(8, "Not found dentry."),
  CFS_STATUS_FILIE_NOT_FOUND(-2, "no such file or directory."),
  CFS_STATUS_NULL_ARGUMENT(-3, "Null argument."),
  CFS_STATUS_EOF(-5, "End-of-file reached");

  public static StatusCodes get(int code)
  {
    switch (code) {
      case 0:
        return CFS_STATUS_OK;
      case 2:
        return CFS_STATUS_TIMEOUT;
      case 3:
        return CFS_STATUS_EOF;
      case 4:
        return CFS_STATUS_FILE_OPEN_FAILED;
      case -17:
        return CFS_STATUS_FILE_EXISTS;
      case 6:
        return CFS_STATUS_IO_ERROR;

      case -2:
        return CFS_STATUS_FILIE_NOT_FOUND;

      case -3:
        return CFS_STATUS_NULL_ARGUMENT;

      default:
        return CFS_STATUS_ERROR;

    }
  }

  private int code;
  private String msg;

  private StatusCodes(int code)
  {
    this.code = code;
    this.msg = "unkown error";
  }

  private StatusCodes(int code, String msg)
  {
    this.code = code;
    this.msg = msg;
  }

  public int code()
  {
    return code;
  }

  public String msg()
  {
    return msg;
  }
}