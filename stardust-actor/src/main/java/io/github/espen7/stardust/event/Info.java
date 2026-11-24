package io.github.espen7.stardust.event;

/** INFO 级别的结构化日志事件。 */
public final class Info extends LogEvent {
    public Info(String source, Class<?> sourceClass, String message) {
        this(source, sourceClass, message, (Throwable) null);
    }

    public Info(String source, Class<?> sourceClass, String message, Throwable cause) {
        super(Level.INFO, source, sourceClass, message, cause);
    }

    /** 创建带 marker 的 INFO 事件。 */
    public Info(String source, Class<?> sourceClass, String message, LogMarker marker) {
        super(Level.INFO, source, sourceClass, message, null, marker);
    }

    /** 创建带 cause 和 marker 的 INFO 事件。 */
    public Info(String source, Class<?> sourceClass, String message, Throwable cause, LogMarker marker) {
        super(Level.INFO, source, sourceClass, message, cause, marker);
    }
}
