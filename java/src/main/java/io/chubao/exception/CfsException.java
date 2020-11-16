package io.chubao.exception;

public class CfsException extends  Exception{
    private static final long serialVersionUID = 1L;
    protected  int code;
    public CfsException(String msg, int code) {
        super(msg);
        this.code = code;
    }
}
