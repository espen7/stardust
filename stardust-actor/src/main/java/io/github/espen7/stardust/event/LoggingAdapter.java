package io.github.espen7.stardust.event;

import java.util.Objects;
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MessageFormatter;

/**
 * 面向 Actor 和应用代码的日志 facade。
 *
 * <p>方法沿用 Pekko 的 debug、info、warning、error 命名和 {@code {}} 参数模板。
 * 调用会生成 LogEvent 并发布到所属 ActorSystem 的 EventStream。</p>
 */
public final class LoggingAdapter {
    private final EventStream eventStream;
    private final String source;
    private final Class<?> sourceClass;

    LoggingAdapter(EventStream eventStream, String source, Class<?> sourceClass) {
        this.eventStream = Objects.requireNonNull(eventStream, "eventStream");
        this.source = Objects.requireNonNull(source, "source");
        this.sourceClass = Objects.requireNonNull(sourceClass, "sourceClass");
    }

    public boolean isDebugEnabled() {
        return eventStream.isEnabled(LogEvent.Level.DEBUG);
    }

    public boolean isInfoEnabled() {
        return eventStream.isEnabled(LogEvent.Level.INFO);
    }

    public boolean isWarningEnabled() {
        return eventStream.isEnabled(LogEvent.Level.WARNING);
    }

    public boolean isErrorEnabled() {
        return eventStream.isEnabled(LogEvent.Level.ERROR);
    }

    public void debug(String message) {
        publish(LogEvent.Level.DEBUG, message, null);
    }

    public void debug(String template, Object... arguments) {
        publish(LogEvent.Level.DEBUG, template, null, arguments);
    }

    public void debug(Throwable cause, String template, Object... arguments) {
        publish(LogEvent.Level.DEBUG, template, cause, arguments);
    }

    public void info(String message) {
        publish(LogEvent.Level.INFO, message, null);
    }

    public void info(String template, Object... arguments) {
        publish(LogEvent.Level.INFO, template, null, arguments);
    }

    public void info(Throwable cause, String template, Object... arguments) {
        publish(LogEvent.Level.INFO, template, cause, arguments);
    }

    public void warning(String message) {
        publish(LogEvent.Level.WARNING, message, null);
    }

    public void warning(String template, Object... arguments) {
        publish(LogEvent.Level.WARNING, template, null, arguments);
    }

    public void warning(Throwable cause, String template, Object... arguments) {
        publish(LogEvent.Level.WARNING, template, cause, arguments);
    }

    public void error(String message) {
        publish(LogEvent.Level.ERROR, message, null);
    }

    public void error(String template, Object... arguments) {
        publish(LogEvent.Level.ERROR, template, null, arguments);
    }

    public void error(Throwable cause, String template, Object... arguments) {
        publish(LogEvent.Level.ERROR, template, cause, arguments);
    }

    private void publish(LogEvent.Level level, String template, Throwable cause, Object... arguments) {
        if (!eventStream.isEnabled(level)) return;
        FormattingTuple formatted = MessageFormatter.arrayFormat(template, arguments);
        String message = formatted.getMessage();
        if (cause == null) cause = formatted.getThrowable();
        LogEvent event = switch (level) {
            case DEBUG -> new Debug(source, sourceClass, message, cause);
            case INFO -> new Info(source, sourceClass, message, cause);
            case WARNING -> new Warning(source, sourceClass, message, cause);
            case ERROR -> new Error(source, sourceClass, message, cause);
        };
        eventStream.publish(event);
    }

}
