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
}
