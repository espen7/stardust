package io.github.espen7.stardust.event;

/**
 * DEBUG 级别的结构化日志事件。
 */
public final class Debug extends LogEvent {
    public Debug(String source, Class<?> sourceClass, String message) {
        this(source, sourceClass, message, (Throwable) null);
    }

    public Debug(String source, Class<?> sourceClass, String message, Throwable cause) {
        super(Level.DEBUG, source, sourceClass, message, cause);
    }

    /** 创建带 marker 的 DEBUG 事件。 */
    public Debug(String source, Class<?> sourceClass, String message, LogMarker marker) {
        super(Level.DEBUG, source, sourceClass, message, null, marker);
    }

    /** 创建带 cause 和 marker 的 DEBUG 事件。 */
    public Debug(String source, Class<?> sourceClass, String message, Throwable cause, LogMarker marker) {
        super(Level.DEBUG, source, sourceClass, message, cause, marker);
    }
}
