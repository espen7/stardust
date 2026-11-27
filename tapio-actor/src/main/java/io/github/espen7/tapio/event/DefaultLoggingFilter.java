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

import io.github.espen7.tapio.actor.ActorSystem;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * 默认 {@link LoggingFilter} 实现，基于 {@link EventStream#logLevel()} 动态判断日志级别。
 */
public class DefaultLoggingFilter implements LoggingFilter {
    private final Supplier<LogEvent.Level> logLevelSupplier;

    /**
     * 使用动态日志级别提供者创建过滤器。
     *
     * @param logLevelSupplier 日志级别提供者
     */
    public DefaultLoggingFilter(Supplier<LogEvent.Level> logLevelSupplier) {
        this.logLevelSupplier = Objects.requireNonNull(logLevelSupplier, "logLevelSupplier must not be null");
    }

    /**
     * 按配置反射契约创建默认日志过滤器。
     *
     * @param settings 系统设置
     * @param eventStream 事件总线
     */
    public DefaultLoggingFilter(ActorSystem.Settings settings, EventStream eventStream) {
        this(Objects.requireNonNull(eventStream, "eventStream must not be null")::logLevel);
    }

    private boolean isEnabled(LogEvent.Level level) {
        LogEvent.Level current = logLevelSupplier.get();
        return current != LogEvent.Level.OFF && level.ordinal() >= current.ordinal();
    }

    @Override
    public boolean isErrorEnabled(Class<?> logClass, String logSource) {
        return isEnabled(LogEvent.Level.ERROR);
    }

    @Override
    public boolean isWarningEnabled(Class<?> logClass, String logSource) {
        return isEnabled(LogEvent.Level.WARNING);
    }

    @Override
    public boolean isInfoEnabled(Class<?> logClass, String logSource) {
        return isEnabled(LogEvent.Level.INFO);
    }

    @Override
    public boolean isDebugEnabled(Class<?> logClass, String logSource) {
        return isEnabled(LogEvent.Level.DEBUG);
    }
}
