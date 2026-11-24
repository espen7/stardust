package io.github.espen7.stardust.event;

/**
 * DEBUG 级别的结构化日志事件。
 */
public final class Debug extends LogEvent {
    public Debug(String source, Class<?> sourceClass, String message) {
        this(source, sourceClass, message, null);
    }

    public Debug(String source, Class<?> sourceClass, String message, Throwable cause) {
        super(Level.DEBUG, source, sourceClass, message, cause);
    }
}
