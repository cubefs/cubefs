package io.chubao.exception;

public enum StatusCodes {
    CFS_STATUS_OK(0, "Ok"),
    CFS_STATUS_ERROR(-999, "Error"),
    CFS_STATUS_FILE_NOT_FOUND(-2, "no such file or directory."),
    CFS_STATUS_IO(-5,"I/O error");

    public static StatusCodes get(int code) {
        switch (code) {
            case 0:
                return CFS_STATUS_OK;
            case -2:
                return CFS_STATUS_FILE_NOT_FOUND;
            case -5:
                return CFS_STATUS_IO;
            default:
                return CFS_STATUS_ERROR;
        }
    }

    private int code;
    private String msg;

    StatusCodes(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }
    public int code() {
        return code;
    }
}