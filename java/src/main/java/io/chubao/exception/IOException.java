package io.chubao.exception;


import static io.chubao.exception.StatusCodes.CFS_STATUS_IO;

public class IOException extends CfsException {
    public IOException(String msg) {
        super(msg, CFS_STATUS_IO.code());
    }
}
