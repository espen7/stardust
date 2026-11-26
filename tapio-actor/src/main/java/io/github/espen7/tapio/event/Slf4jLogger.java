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

import io.github.espen7.tapio.actor.AbstractActor;
import io.github.espen7.tapio.actor.ActorSystemImpl;
import io.github.espen7.tapio.dispatch.RequiresMessageQueue;
import io.github.espen7.tapio.util.Helpers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.slf4j.MDC;

import java.util.function.BiConsumer;

/**
 * 将 EventStream 中的日志事件桥接到应用选择的 SLF4J provider。
 *
 * <p>可通过 {@code tapio.loggers} 配置为默认 logger；级别过滤由 EventStream 的
 * channel 订阅负责，未处理消息由独立的 UnhandledMessageForwarder 转换。</p>
 */
public class Slf4jLogger extends AbstractActor
        implements RequiresMessageQueue<LoggerMessageQueueSemantics> {
    public final String mdcThreadAttributeName = "sourceThread";
    public final String mdcActorSystemAttributeName = "sourceActorSystem";
    public final String mdcPekkoSourceAttributeName = "pekkoSource";
    public final String mdcPekkoTimestamp = "pekkoTimestamp";
    public final String mdcPekkoAddressAttributeName = "pekkoAddress";
    public final String mdcPekkoUidAttributeName = "pekkoUid";
    private final Logger log = LoggerFactory.getLogger(Slf4jLogger.class);
    private String _pekkoUid = "";

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(InitializeLogger.class, ignored -> {
                    log.info("Slf4jLogger started");
                    getSender().tell(LoggerInitialized.getInstance());
                })
                .match(LogEvent.class, this::publish)
                .build();
    }

    private void publish(LogEvent event) {
        withMdc(event.source(), event,
                () -> publish(LoggerFactory.getLogger(event.sourceClass()), markerIfPresent(event), event));
    }

    /**
     * 在日志调用前写入标准 MDC 和事件 MDC，并在结束后清空。
     *
     * @param logSource 日志 source
     * @param logEvent 日志事件
     * @param logStatement 日志调用
     */
    public final void withMdc(String logSource, LogEvent logEvent, Runnable logStatement) {
        ActorSystemImpl system = (ActorSystemImpl) getContext().system();
        withMdc(logEvent, logSource, system.name(), pekkoAddress(), pekkoUid(), MDC::put, MDC::clear, logStatement);
    }

    final void withMdc(LogEvent event, String logSource, String systemName, String address, String uid,
                      BiConsumer<String, String> put, Runnable clear, Runnable statement) {
        if (event.marker() != null) {
            event.marker().properties().forEach((key, value) -> put.accept(key, String.valueOf(value)));
        }
        put.accept(mdcPekkoSourceAttributeName, logSource);
        put.accept(mdcThreadAttributeName, event.threadName());
        put.accept(mdcPekkoTimestamp, formatTimestamp(event.timestamp()));
        put.accept(mdcActorSystemAttributeName, systemName);
        put.accept(mdcPekkoAddressAttributeName, address);
        if (!uid.isEmpty()) put.accept(mdcPekkoUidAttributeName, uid);
        event.mdc().forEach((key, value) -> put.accept(key, String.valueOf(value)));
        try {
            statement.run();
        } finally {
            clear.run();
        }
    }

    private String pekkoAddress() {
        return ((ActorSystemImpl) getContext().system()).provider().getDefaultAddress().toString();
    }

    private String pekkoUid() {
        if (_pekkoUid.isEmpty()) {
            try {
                _pekkoUid = Long.toString(((ActorSystemImpl) getContext().system()).uid());
            } catch (IllegalStateException ignored) {
            }
        }
        return _pekkoUid;
    }

    static Marker markerIfPresent(LogEvent event) {
        if (event.marker() == null) return null;
        if (event.marker() instanceof Slf4jLogMarker slf4jMarker) return slf4jMarker.marker();
        return MarkerFactory.getMarker(event.marker().name());
    }

    static void publish(Logger logger, Marker marker, LogEvent event) {
        String message = event.message();
        switch (event.level()) {
            case DEBUG -> publishDebug(logger, marker, message);
            case INFO -> publishInfo(logger, marker, message);
            case WARNING -> {
                if (event.cause() == null) publishWarning(logger, marker, message);
                else if (marker == null) logger.warn(message, event.cause());
                else logger.warn(marker, message, event.cause());
            }
            case ERROR -> {
                if (event.cause() == null) publishError(logger, marker, message);
                else if (marker == null) logger.error(message, event.cause());
                else logger.error(marker, message, event.cause());
            }
            default -> {
            }
        }
    }

    private static void publishDebug(Logger logger, Marker marker, String message) {
        if (marker == null) logger.debug(message);
        else logger.debug(marker, message);
    }

    private static void publishInfo(Logger logger, Marker marker, String message) {
        if (marker == null) logger.info(message);
        else logger.info(marker, message);
    }

    private static void publishWarning(Logger logger, Marker marker, String message) {
        if (marker == null) logger.warn(message);
        else logger.warn(marker, message);
    }

    private static void publishError(Logger logger, Marker marker, String message) {
        if (marker == null) logger.error(message);
        else logger.error(marker, message);
    }

    /**
     * 格式化 UTC 时间戳，可由子类覆盖。
     *
     * @param timestamp 毫秒时间戳
     * @return UTC 时间文本
     */
    protected String formatTimestamp(long timestamp) {
        return Helpers.currentTimeMillisToUTCString(timestamp);
    }
}
