package io.chubao.exception;

import static io.chubao.exception.StatusCodes.CFS_STATUS_FILE_NOT_FOUND;

public class FileNotFoundException extends CfsException {
    public FileNotFoundException(String msg) {
        super(msg, CFS_STATUS_FILE_NOT_FOUND.code());
    }
}
