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

import java.util.Objects;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSystemImpl;
import io.github.espen7.tapio.actor.Props;

/** 创建绑定 EventStream 和 source 元数据的 LoggingAdapter。 */
public final class Logging {
    private Logging() {
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
     * 创建系统 logger Actor，并订阅日志事件。
     *
     * @param system 所属 ActorSystem
     * @param debugUnhandled 是否将 UnhandledMessage 转成 DEBUG 日志
     * @return logger ActorRef
     */
    public static ActorRef installSlf4jLogger(ActorSystemImpl system, boolean debugUnhandled) {
        EventStream eventStream = system.eventStream();
        ActorRef logger = system.systemActorOf(
                Props.create(Slf4jLogger.class, eventStream.logLevel(), debugUnhandled), "eventStreamLogger");
        eventStream.subscribe(logger, LogEvent.class);
        if (debugUnhandled) {
            eventStream.subscribe(logger, io.github.espen7.tapio.actor.UnhandledMessage.class);
        }
        return logger;
    }
}
