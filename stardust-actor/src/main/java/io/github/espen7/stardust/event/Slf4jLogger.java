package io.github.espen7.stardust.event;

import io.github.espen7.stardust.actor.AbstractActor;
import io.github.espen7.stardust.actor.UnhandledMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.slf4j.MDC;


/**
 * 将 EventStream 中的日志事件桥接到应用选择的 SLF4J provider。
 */
final class Slf4jLogger extends AbstractActor {
    private final LogEvent.Level logLevel;
    private final boolean debugUnhandled;

    Slf4jLogger(LogEvent.Level logLevel, boolean debugUnhandled) {
        this.logLevel = logLevel;
        this.debugUnhandled = debugUnhandled;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(LogEvent.class, event -> {
                    if (event.level().ordinal() >= logLevel.ordinal()) publish(event);
                })
                .match(UnhandledMessage.class, event -> {
                    if (debugUnhandled) publishUnhandled(event);
                })
                .build();
    }

    private static void publish(LogEvent event) {
        Logger logger = LoggerFactory.getLogger(event.sourceClass());
        String message = "[" + event.source() + "] " + event.message();
        Marker marker = null;
        if (event.marker() != null) {
            marker = MarkerFactory.getMarker(event.marker().name());
            event.marker().properties().forEach((key, value) -> MDC.put(key, String.valueOf(value)));
        }
        try {
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
            }
        } finally {
            if (event.marker() != null) MDC.clear();
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

    private static void publishUnhandled(UnhandledMessage event) {
        Class<?> actorClass = event.recipient() instanceof io.github.espen7.stardust.actor.LocalActorRef localRef
                ? localRef.underlying().actor().getClass()
                : event.recipient().getClass();
        String message = "unhandled message from " + event.sender() + ": " + event.message();
        publish(new Debug(event.recipient().path().toString(), actorClass, message));
    }
}
