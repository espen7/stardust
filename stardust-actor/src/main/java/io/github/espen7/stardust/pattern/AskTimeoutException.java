package io.github.espen7.stardust.pattern;

/**
 * ask 在指定 timeout 内没有收到回复时使用的异常。
 */
public class AskTimeoutException extends RuntimeException {
    public AskTimeoutException(String message) {
        super(message);
    }
}
