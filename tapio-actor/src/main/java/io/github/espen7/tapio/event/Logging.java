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
import io.github.espen7.tapio.actor.ExtendedActorSystem;
import io.github.espen7.tapio.util.NoStackTrace;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Objects;

/** 创建绑定 EventStream 和 source 元数据的 LoggingAdapter。 */
public final class Logging {
    private Logging() {
    }

    /**
     * 创建绑定到 {@link ActorSystem} 及其 {@link LoggingFilter} 的 {@link LoggingAdapter}。
     *
     * @param system 目标 ActorSystem
     * @param source 日志 source
     * @param sourceClass 日志 source class
     * @return LoggingAdapter
     */
    public static LoggingAdapter getLogger(ActorSystem system, String source, Class<?> sourceClass) {
        Objects.requireNonNull(system, "system");
        if (system instanceof ExtendedActorSystem extended) {
            return new LoggingAdapter(system.eventStream(), source, sourceClass, extended.logFilter());
        }
        return new LoggingAdapter(system.eventStream(), source, sourceClass);
    }

    /**
     * 创建绑定到 {@link ActorSystem} 及其 {@link LoggingFilter} 的 {@link LoggingAdapter}。
     *
     * @param system 目标 ActorSystem
     * @param source 日志 source 对象
     * @param sourceClass 日志 source class
     * @return LoggingAdapter
     */
    public static LoggingAdapter getLogger(ActorSystem system, Object source, Class<?> sourceClass) {
        return getLogger(system, Objects.toString(source), sourceClass);
    }

    /**
     * 创建带 source 元数据的 LoggingAdapter。
     *
     * @param eventStream 目标 EventStream
     * @param source 日志 source
     * @param sourceClass 日志 source class
     * @return LoggingAdapter
     */
    public static LoggingAdapter getLogger(EventStream eventStream, String source, Class<?> sourceClass) {
        return new LoggingAdapter(eventStream, source, sourceClass);
    }

    /**
     * 创建带对象 source 元数据的 LoggingAdapter。
     *
     * @param eventStream 目标 EventStream
     * @param source 日志 source 对象
     * @param sourceClass 日志 source class
     * @return LoggingAdapter
     */
    public static LoggingAdapter getLogger(EventStream eventStream, Object source, Class<?> sourceClass) {
        return getLogger(eventStream, Objects.toString(source), sourceClass);
    }

    /**
     * 返回对象 class 的 binary simple name。
     *
     * @param obj 源对象
     * @return 去掉包名后的 class 名
     */
    public static String simpleName(Object obj) {
        return simpleName(obj.getClass());
    }

    /**
     * 返回 class 的 binary simple name，保留嵌套 class 的 {@code $} 分隔符。
     *
     * @param clazz 源 class
     * @return 去掉包名后的 class 名
     */
    public static String simpleName(Class<?> clazz) {
        String n = clazz.getName();
        int i = n.lastIndexOf('.');
        return n.substring(i + 1);
    }

    /**
     * 将 Throwable 格式化为日志堆栈文本。
     *
     * @param cause 异常；{@code null} 返回空文本
     * @return 完整堆栈或 {@link NoStackTrace} 异常的短文本
     */
    public static String stackTraceFor(Throwable cause) {
        if (cause == null) return "";
        if (cause instanceof NoStackTrace) {
            return " (" + cause.getClass().getName() + ": " + cause.getMessage() + ")";
        }
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        pw.append('\n');
        cause.printStackTrace(pw);
        return sw.toString();
    }
}
