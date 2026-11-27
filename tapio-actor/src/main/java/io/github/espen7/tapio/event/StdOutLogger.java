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

import io.github.espen7.tapio.util.Helpers;

/** 将结构化日志事件同步输出到标准输出。 */
public interface StdOutLogger {

    /** 输出日志事件；未知事件类型转为 WARNING。 */
    default void print(Object event) {
        if (event instanceof Error error) error(error);
        else if (event instanceof Warning warning) warning(warning);
        else if (event instanceof Info info) info(info);
        else if (event instanceof Debug debug) debug(debug);
        else warning(new Warning(Logging.simpleName(this), getClass(),
                "received unexpected event of class " + event.getClass() + ": " + event));
    }

    /** 输出 ERROR 事件。 */
    default void error(Error event) {
        String marker = marker(event);
        String trace = Logging.stackTraceFor(event.cause());
        System.out.printf("[ERROR] %s[%s] [%s] [%s]%s %s%s%n", marker, timestamp(event),
                event.threadName(), event.source(), formatMDC(event), event.message(), trace);
    }

    /** 输出 WARNING 事件。 */
    default void warning(Warning event) {
        System.out.printf("[WARN] %s[%s] [%s] [%s]%s %s%n", marker(event), timestamp(event),
                event.threadName(), event.source(), formatMDC(event), event.message());
    }

    /** 输出 INFO 事件。 */
    default void info(Info event) {
        System.out.printf("[INFO] %s[%s] [%s] [%s]%s %s%n", marker(event), timestamp(event),
                event.threadName(), event.source(), formatMDC(event), event.message());
    }

    /** 输出 DEBUG 事件。 */
    default void debug(Debug event) {
        System.out.printf("[DEBUG] %s[%s] [%s] [%s]%s %s%n", marker(event), timestamp(event),
                event.threadName(), event.source(), formatMDC(event), event.message());
    }

    /**
     * 格式化事件时间戳，可由实现覆盖。
     *
     * @param event 日志事件
     * @return 时间戳文本
     */
    default String timestamp(LogEvent event) {
        return Helpers.timestamp(event.timestamp());
    }

    private static String marker(LogEvent event) {
        return event.marker() == null ? "" : "[" + event.marker().name() + "]";
    }

    private static String formatMDC(LogEvent event) {
        if (event.mdc().isEmpty()) return "";
        StringBuilder result = new StringBuilder();
        event.mdc().forEach((key, value) -> {
            if (!result.isEmpty()) result.append("][");
            result.append(key).append(':').append(value);
        });
        return "[" + result + "]";
    }

}
