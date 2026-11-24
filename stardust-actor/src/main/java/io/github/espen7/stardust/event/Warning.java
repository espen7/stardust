package io.github.espen7.stardust.event;

/**
 * WARNING 级别的结构化日志事件。
 */
public final class Warning extends LogEvent {
    public Warning(String source, Class<?> sourceClass, String message) {
        super(Level.WARNING, source, sourceClass, message, null);
    }

    public Warning(String source, Class<?> sourceClass, String message, Throwable cause) {
        super(Level.WARNING, source, sourceClass, message, cause);
    }

    /** 创建带 marker 的 WARNING 事件。 */
    public Warning(String source, Class<?> sourceClass, String message, LogMarker marker) {
        super(Level.WARNING, source, sourceClass, message, null, marker);
    }

    /** 创建带 cause 和 marker 的 WARNING 事件。 */
    public Warning(String source, Class<?> sourceClass, String message, Throwable cause, LogMarker marker) {
        super(Level.WARNING, source, sourceClass, message, cause, marker);
    }
}
