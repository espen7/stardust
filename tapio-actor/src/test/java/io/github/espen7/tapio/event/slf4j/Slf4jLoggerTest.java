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
package io.github.espen7.tapio.event.slf4j;

import io.github.espen7.tapio.event.Info;
import io.github.espen7.tapio.event.LogEvent;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Slf4jLoggerTest {
    @Test
    void slf4jLoggerPreservesMessageText() {
        AtomicReference<String> message = new AtomicReference<>();
        Logger logger = (Logger) Proxy.newProxyInstance(Logger.class.getClassLoader(), new Class<?>[]{Logger.class},
                (proxy, method, arguments) -> {
                    if ("info".equals(method.getName())) {
                        message.set(arguments.length > 1 ? (String) arguments[1] : (String) arguments[0]);
                    }
                    return null;
                });

        Slf4jLogger.publish(logger, null,
                new Info("test-source", Slf4jLoggerTest.class, "plain-message"));

        assertEquals("plain-message", message.get());
    }

    @Test
    void slf4jLoggerPreservesWrappedMarker() {
        Marker parent = MarkerFactory.getMarker("parent-marker");
        Marker child = MarkerFactory.getMarker("child-marker");
        child.add(parent);
        LogEvent event = new Info("test-source", Slf4jLoggerTest.class, "marker-message",
                Slf4jLogMarker.create(child));

        assertSame(child, Slf4jLogger.markerIfPresent(event));
        assertTrue(event.marker().properties().isEmpty());
    }
}
