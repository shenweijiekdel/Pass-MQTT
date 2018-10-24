package cn.com.bjfanuc.exception;

/**
 * 数据错误异常定义
 */
public class DataErrException extends Exception {
    public DataErrException(String name) {
        super(name);
    }
}
