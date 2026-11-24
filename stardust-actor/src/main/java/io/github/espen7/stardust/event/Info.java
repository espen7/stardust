package io.github.espen7.stardust.event;

/** INFO 级别的结构化日志事件。 */
public final class Info extends LogEvent {
    public Info(String source, Class<?> sourceClass, String message) {
        this(source, sourceClass, message, null);
    }

    public Info(String source, Class<?> sourceClass, String message, Throwable cause) {
        super(Level.INFO, source, sourceClass, message, cause);
    }
}
