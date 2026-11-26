/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.espen7.tapio.event;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * EventStream 中发布的结构化日志事件。
 *
 * <p>source、sourceClass 和 message 分开保存，日志后端可以决定如何展示 source 元数据，
 * 同时订阅者也能在不解析格式化文本的情况下检查事件。</p>
 */
public abstract class LogEvent implements LogEventWithMarker {
    /** 日志级别，顺序同时表示过滤优先级。 */
    public enum Level {
        /** 关闭日志。 */
        OFF,
        /** DEBUG 日志。 */
        DEBUG,
        /** INFO 日志。 */
        INFO,
        /** WARNING 日志。 */
        WARNING,
        /** ERROR 日志。 */
        ERROR;

        /**
         * 将配置中的级别名称解析为对应的日志级别。
         *
         * @param value 配置级别名称
         * @return 对应日志级别
         * @throws IllegalArgumentException 名称未知时抛出
         */
        public static Level parse(String value) {
            return switch (value.toUpperCase(java.util.Locale.ROOT)) {
                case "OFF" -> OFF;
                case "DEBUG" -> DEBUG;
                case "INFO" -> INFO;
                case "WARNING", "WARN" -> WARNING;
                case "ERROR" -> ERROR;
                default -> throw new IllegalArgumentException("Unknown log level: " + value);
            };
        }
    }

    private final Level level;
    private final long timestamp;
    private final String threadName;
    private final String source;
    private final Class<?> sourceClass;
    private final String message;
    private final Throwable cause;
    private final LogMarker marker;
    private final Map<String, Object> mdc;

    /**
     * 创建不带 marker 的日志事件。
     *
     * @param level 日志级别
     * @param source 日志 source
     * @param sourceClass 日志 source class
     * @param message 日志消息
     * @param cause 异常原因
     */
    protected LogEvent(Level level, String source, Class<?> sourceClass, String message, Throwable cause) {
        this(level, source, sourceClass, message, cause, null);
    }

    /**
     * 创建带 marker 的日志事件。
     *
     * @param level 日志级别
     * @param source 日志 source
     * @param sourceClass 日志 source class
     * @param message 日志消息
     * @param cause 异常原因
     * @param marker 日志 marker
     */
    protected LogEvent(Level level, String source, Class<?> sourceClass, String message, Throwable cause,
                       LogMarker marker) {
        this(level, source, sourceClass, message, cause, marker, Map.of());
    }

    /**
     * 创建带 MDC 和 marker 的日志事件。
     *
     * @param level 日志级别
     * @param source 日志 source
     * @param sourceClass 日志 source class
     * @param message 日志消息
     * @param cause 异常原因
     * @param marker 日志 marker
     * @param mdc 写入日志上下文的额外字段
     */
    protected LogEvent(Level level, String source, Class<?> sourceClass, String message, Throwable cause,
                       LogMarker marker, Map<String, Object> mdc) {
        this.level = Objects.requireNonNull(level, "level");
        this.timestamp = System.currentTimeMillis();
        this.threadName = Thread.currentThread().getName();
        this.source = Objects.requireNonNull(source, "source");
        this.sourceClass = Objects.requireNonNull(sourceClass, "sourceClass");
        this.message = Objects.requireNonNull(message, "message");
        this.cause = cause;
        this.marker = marker;
        this.mdc = Collections.unmodifiableMap(new HashMap<>(Objects.requireNonNull(mdc, "mdc")));
    }

    /**
     * 返回日志级别。
     *
     * @return 日志级别
     */
    public final Level level() {
        return level;
    }

    /**
     * 返回事件创建时的毫秒时间戳。
     *
     * @return 毫秒时间戳
     */
    public final long timestamp() {
        return timestamp;
    }

    /**
     * 返回事件创建时的线程名。
     *
     * @return 线程名
     */
    public final String threadName() {
        return threadName;
    }

    /**
     * 返回日志 source。
     *
     * @return 日志 source
     */
    public final String source() {
        return source;
    }

    /**
     * 返回日志 source class。
     *
     * @return 日志 source class
     */
    public final Class<?> sourceClass() {
        return sourceClass;
    }

    /**
     * 返回日志消息。
     *
     * @return 日志消息
     */
    public final String message() {
        return message;
    }

    /**
     * 返回异常原因。
     *
     * @return 异常原因，没有异常时为 {@code null}
     */
    public final Throwable cause() {
        return cause;
    }

    @Override
    /**
     * 返回日志 marker。
     *
     * @return 日志 marker，没有 marker 时为 {@code null}
     */
    public final LogMarker marker() {
        return marker;
    }

    /**
     * 返回事件的只读 MDC 快照。
     *
     * @return 额外的日志上下文字段
     */
    public final Map<String, Object> mdc() {
        return mdc;
    }

    /**
     * 返回事件的只读 MDC 快照。
     *
     * @return 额外的日志上下文字段
     */
    public final Map<String, Object> getMDC() {
        return mdc;
    }
}
