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
package io.github.espen7.tapio.dispatch;

import io.github.espen7.tapio.event.EventStream;
import io.github.espen7.tapio.event.LogEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** 避免失败事件经 EventStream 重新进入 Dispatcher 时无限递归。 */
final class DispatchFailureReporting {
    private static final Set<Thread> REPORTING = ConcurrentHashMap.<Thread>newKeySet();

    private DispatchFailureReporting() {
    }

    static void report(EventStream eventStream, LogEvent event) {
        Thread thread = Thread.currentThread();
        if (!REPORTING.add(thread)) {
            publishToSlf4j(event);
            return;
        }
        try {
            eventStream.publish(event);
        } finally {
            REPORTING.remove(thread);
        }
    }

    private static void publishToSlf4j(LogEvent event) {
        Logger logger = LoggerFactory.getLogger(event.sourceClass());
        String message = "[" + event.source() + "] " + event.message();
        switch (event.level()) {
            case DEBUG -> logger.debug(message, event.cause());
            case INFO -> logger.info(message, event.cause());
            case WARNING -> logger.warn(message, event.cause());
            case ERROR -> logger.error(message, event.cause());
        }
    }
}
