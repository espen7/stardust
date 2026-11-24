package io.github.espen7.stardust.event;

import java.util.Objects;

/**
 * EventStream 中发布的结构化日志事件。
 *
 * <p>source、sourceClass 和 message 分开保存，日志后端可以决定如何展示 source 元数据，
 * 同时订阅者也能在不解析格式化文本的情况下检查事件。</p>
 */
public abstract class LogEvent {
    public enum Level {
        DEBUG,
        INFO,
        WARNING,
        ERROR;

        /** 将配置中的级别名称解析为对应的日志级别。 */
        public static Level parse(String value) {
            return switch (value.toUpperCase(java.util.Locale.ROOT)) {
                case "DEBUG" -> DEBUG;
                case "INFO" -> INFO;
                case "WARNING", "WARN" -> WARNING;
                case "ERROR" -> ERROR;
                default -> throw new IllegalArgumentException("Unknown log level: " + value);
            };
        }
    }

    private final Level level;
    private final String source;
    private final Class<?> sourceClass;
    private final String message;
    private final Throwable cause;

    protected LogEvent(Level level, String source, Class<?> sourceClass, String message, Throwable cause) {
        this.level = Objects.requireNonNull(level, "level");
        this.source = Objects.requireNonNull(source, "source");
        this.sourceClass = Objects.requireNonNull(sourceClass, "sourceClass");
        this.message = Objects.requireNonNull(message, "message");
        this.cause = cause;
    }

    public final Level level() {
        return level;
    }

    public final String source() {
        return source;
    }

    public final Class<?> sourceClass() {
        return sourceClass;
    }

    public final String message() {
        return message;
    }

    public final Throwable cause() {
        return cause;
    }
}
