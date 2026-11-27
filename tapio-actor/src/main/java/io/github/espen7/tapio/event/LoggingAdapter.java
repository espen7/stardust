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

import java.lang.reflect.Array;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

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
    private final LoggingFilter loggingFilter;
    private Map<String, Object> mdc = Map.of();

    LoggingAdapter(EventStream eventStream, String source, Class<?> sourceClass) {
        this(eventStream, source, sourceClass,
                new DefaultLoggingFilter(Objects.requireNonNull(eventStream, "eventStream")::logLevel));
    }

    LoggingAdapter(EventStream eventStream, String source, Class<?> sourceClass, LoggingFilter loggingFilter) {
        this.eventStream = Objects.requireNonNull(eventStream, "eventStream");
        this.source = Objects.requireNonNull(source, "source");
        this.sourceClass = Objects.requireNonNull(sourceClass, "sourceClass");
        this.loggingFilter = Objects.requireNonNull(loggingFilter, "loggingFilter");
    }

    /**
     * 返回 DEBUG 日志是否启用。
     *
     * @return DEBUG 日志是否启用
     */
    public boolean isDebugEnabled() {
        return loggingFilter.isDebugEnabled(sourceClass, source);
    }

    /**
     * 返回 INFO 日志是否启用。
     *
     * @return INFO 日志是否启用
     */
    public boolean isInfoEnabled() {
        return loggingFilter.isInfoEnabled(sourceClass, source);
    }

    /**
     * 返回 WARNING 日志是否启用。
     *
     * @return WARNING 日志是否启用
     */
    public boolean isWarningEnabled() {
        return loggingFilter.isWarningEnabled(sourceClass, source);
    }

    /**
     * 返回 ERROR 日志是否启用。
     *
     * @return ERROR 日志是否启用
     */
    public boolean isErrorEnabled() {
        return loggingFilter.isErrorEnabled(sourceClass, source);
    }

    /**
     * 返回指定 marker 下 DEBUG 是否启用。
     *
     * @param marker 日志 marker
     * @return DEBUG 日志是否启用
     */
    public boolean isDebugEnabled(LogMarker marker) {
        return isDebugEnabled();
    }

    /**
     * 返回指定 marker 下 INFO 是否启用。
     *
     * @param marker 日志 marker
     * @return INFO 日志是否启用
     */
    public boolean isInfoEnabled(LogMarker marker) {
        return isInfoEnabled();
    }

    /**
     * 返回指定 marker 下 WARNING 是否启用。
     *
     * @param marker 日志 marker
     * @return WARNING 日志是否启用
     */
    public boolean isWarningEnabled(LogMarker marker) {
        return isWarningEnabled();
    }

    /**
     * 返回指定 marker 下 ERROR 是否启用。
     *
     * @param marker 日志 marker
     * @return ERROR 日志是否启用
     */
    public boolean isErrorEnabled(LogMarker marker) {
        return isErrorEnabled();
    }

    /**
     * 返回当前日志上下文的只读快照。
     *
     * @return MDC 字段
     */
    public Map<String, Object> mdc() {
        return Collections.unmodifiableMap(new HashMap<>(mdc));
    }

    /**
     * 设置后续日志事件的 MDC 字段。
     *
     * @param mdc MDC 字段；{@code null} 表示清空
     */
    public void mdc(Map<String, Object> mdc) {
        this.mdc = mdc == null ? Map.of() : Collections.unmodifiableMap(new HashMap<>(mdc));
    }

    /**
     * 返回当前日志上下文的只读快照。
     *
     * @return MDC 字段
     */
    public Map<String, Object> getMDC() {
        return mdc();
    }

    /**
     * 设置后续日志事件的 MDC 字段。
     *
     * @param mdc MDC 字段；{@code null} 表示清空
     */
    public void setMDC(Map<String, Object> mdc) {
        mdc(mdc);
    }

    /**
     * 清空后续日志事件的 MDC 字段。
     */
    public void clearMDC() {
        mdc = Map.of();
    }

    /**
     * 发布 DEBUG 日志。
     *
     * @param message DEBUG 日志消息
     */
    public void debug(String message) {
        publish(LogEvent.Level.DEBUG, message, null);
    }

    /**
     * 按模板发布 DEBUG 日志。
     *
     * @param template DEBUG 消息模板
     * @param arguments 模板参数
     */
    public void debug(String template, Object... arguments) {
        publish(LogEvent.Level.DEBUG, template, null, arguments);
    }

    /**
     * 按模板发布带异常的 DEBUG 日志。
     *
     * @param cause 异常原因
     * @param template 消息模板
     * @param arguments 模板参数
     */
    public void debug(Throwable cause, String template, Object... arguments) {
        publish(LogEvent.Level.DEBUG, template, cause, arguments);
    }

    /**
     * 使用 marker 发布 DEBUG 日志。
     *
     * @param marker 日志 marker
     * @param message 日志消息
     */
    public void debug(LogMarker marker, String message) {
        publish(LogEvent.Level.DEBUG, message, null, marker);
    }

    /**
     * 使用 marker 和 cause 发布 DEBUG 日志。
     *
     * @param marker 日志 marker
     * @param cause 异常原因
     * @param template 消息模板
     * @param arguments 模板参数
     */
    public void debug(LogMarker marker, Throwable cause, String template, Object... arguments) {
        publish(LogEvent.Level.DEBUG, template, cause, marker, arguments);
    }

    /**
     * 发布 INFO 日志。
     *
     * @param message INFO 日志消息
     */
    public void info(String message) {
        publish(LogEvent.Level.INFO, message, null);
    }

    /**
     * 按模板发布 INFO 日志。
     *
     * @param template INFO 消息模板
     * @param arguments 模板参数
     */
    public void info(String template, Object... arguments) {
        publish(LogEvent.Level.INFO, template, null, arguments);
    }

    /**
     * 按模板发布带异常的 INFO 日志。
     *
     * @param cause 异常原因
     * @param template 消息模板
     * @param arguments 模板参数
     */
    public void info(Throwable cause, String template, Object... arguments) {
        publish(LogEvent.Level.INFO, template, cause, arguments);
    }

    /**
     * 使用 marker 发布 INFO 日志。
     *
     * @param marker 日志 marker
     * @param message 日志消息
     */
    public void info(LogMarker marker, String message) {
        publish(LogEvent.Level.INFO, message, null, marker);
    }

    /**
     * 使用 marker 和 cause 发布 INFO 日志。
     *
     * @param marker 日志 marker
     * @param cause 异常原因
     * @param template 消息模板
     * @param arguments 模板参数
     */
    public void info(LogMarker marker, Throwable cause, String template, Object... arguments) {
        publish(LogEvent.Level.INFO, template, cause, marker, arguments);
    }

    /**
     * 发布 WARNING 日志。
     *
     * @param message WARNING 日志消息
     */
    public void warning(String message) {
        publish(LogEvent.Level.WARNING, message, null);
    }

    /**
     * 按模板发布 WARNING 日志。
     *
     * @param template WARNING 消息模板
     * @param arguments 模板参数
     */
    public void warning(String template, Object... arguments) {
        publish(LogEvent.Level.WARNING, template, null, arguments);
    }

    /**
     * 按模板发布带异常的 WARNING 日志。
     *
     * @param cause 异常原因
     * @param template 消息模板
     * @param arguments 模板参数
     */
    public void warning(Throwable cause, String template, Object... arguments) {
        publish(LogEvent.Level.WARNING, template, cause, arguments);
    }

    /**
     * 使用 marker 发布 WARNING 日志。
     *
     * @param marker 日志 marker
     * @param message 日志消息
     */
    public void warning(LogMarker marker, String message) {
        publish(LogEvent.Level.WARNING, message, null, marker);
    }

    /**
     * 使用 marker 和 cause 发布 WARNING 日志。
     *
     * @param marker 日志 marker
     * @param cause 异常原因
     * @param template 消息模板
     * @param arguments 模板参数
     */
    public void warning(LogMarker marker, Throwable cause, String template, Object... arguments) {
        publish(LogEvent.Level.WARNING, template, cause, marker, arguments);
    }

    /**
     * 发布 ERROR 日志。
     *
     * @param message ERROR 日志消息
     */
    public void error(String message) {
        publish(LogEvent.Level.ERROR, message, null);
    }

    /**
     * 按模板发布 ERROR 日志。
     *
     * @param template ERROR 消息模板
     * @param arguments 模板参数
     */
    public void error(String template, Object... arguments) {
        publish(LogEvent.Level.ERROR, template, null, arguments);
    }

    /**
     * 按模板发布带异常的 ERROR 日志。
     *
     * @param cause 异常原因
     * @param template 消息模板
     * @param arguments 模板参数
     */
    public void error(Throwable cause, String template, Object... arguments) {
        publish(LogEvent.Level.ERROR, template, cause, arguments);
    }

    /**
     * 使用 marker 发布 ERROR 日志。
     *
     * @param marker 日志 marker
     * @param message 日志消息
     */
    public void error(LogMarker marker, String message) {
        publish(LogEvent.Level.ERROR, message, null, marker);
    }

    /**
     * 使用 marker 和 cause 发布 ERROR 日志。
     *
     * @param marker 日志 marker
     * @param cause 异常原因
     * @param template 消息模板
     * @param arguments 模板参数
     */
    public void error(LogMarker marker, Throwable cause, String template, Object... arguments) {
        publish(LogEvent.Level.ERROR, template, cause, marker, arguments);
    }

    private void publish(LogEvent.Level level, String template, Throwable cause, Object... arguments) {
        publish(level, template, cause, null, arguments);
    }

    private void publish(LogEvent.Level level, String template, Throwable cause, LogMarker marker,
                         Object... arguments) {
        boolean enabled = switch (level) {
            case DEBUG -> isDebugEnabled();
            case INFO -> isInfoEnabled();
            case WARNING -> isWarningEnabled();
            case ERROR -> isErrorEnabled();
            case OFF -> false;
        };
        if (!enabled) return;
        String message = format(template, arguments);
        LogEvent event = switch (level) {
            case DEBUG -> new Debug(source, sourceClass, message, cause, marker, mdc);
            case INFO -> new Info(source, sourceClass, message, cause, marker, mdc);
            case WARNING -> new Warning(source, sourceClass, message, cause, marker, mdc);
            case ERROR -> new Error(source, sourceClass, message, cause, marker, mdc);
            case OFF -> throw new IllegalArgumentException("OFF is not a log event level");
        };
        eventStream.publish(event);
    }

    private static String format(String template, Object[] arguments) {
        if (arguments == null) arguments = new Object[]{null};
        if (arguments.length == 1 && arguments[0] != null && arguments[0].getClass().isArray()) {
            Object array = arguments[0];
            Object[] expanded = new Object[Array.getLength(array)];
            for (int i = 0; i < expanded.length; i++) expanded[i] = Array.get(array, i);
            arguments = expanded;
        }
        StringBuilder sb = new StringBuilder(64);
        int p = 0;
        int startIndex = 0;
        while (p < arguments.length) {
            int index = template.indexOf("{}", startIndex);
            if (index == -1) {
                sb.append(template.substring(startIndex, template.length()))
                        .append(" WARNING arguments left: ").append(arguments.length - p);
                p = arguments.length;
                startIndex = template.length();
            } else {
                sb.append(template, startIndex, index).append(arguments[p]);
                startIndex = index + 2;
                p += 1;
            }
        }
        return sb.append(template.substring(startIndex)).toString();
    }

}
