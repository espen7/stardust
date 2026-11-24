package io.github.espen7.stardust.event;

import io.github.espen7.stardust.actor.UnhandledMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/**
 * 将 EventStream 中的日志事件桥接到应用选择的 SLF4J provider。
 */
final class Slf4jLogger implements Consumer<Object>, AutoCloseable {
    private final EventStream eventStream;
    private final boolean debugUnhandled;

    Slf4jLogger(EventStream eventStream, boolean debugUnhandled) {
        this.eventStream = eventStream;
        this.debugUnhandled = debugUnhandled;
    }

    @Override
    public void accept(Object event) {
        if (event instanceof LogEvent logEvent) publish(logEvent);
        else if (debugUnhandled && event instanceof UnhandledMessage unhandled) publishUnhandled(unhandled);
    }

    private static void publish(LogEvent event) {
        Logger logger = LoggerFactory.getLogger(event.sourceClass());
        String message = "[" + event.source() + "] " + event.message();
        switch (event.level()) {
            case DEBUG -> logger.debug(message);
            case INFO -> logger.info(message);
            case WARNING -> {
                if (event.cause() == null) logger.warn(message);
                else logger.warn(message, event.cause());
            }
            case ERROR -> {
                if (event.cause() == null) logger.error(message);
                else logger.error(message, event.cause());
            }
        }
    }

    private static void publishUnhandled(UnhandledMessage event) {
        Class<?> actorClass = event.recipient() instanceof io.github.espen7.stardust.actor.LocalActorRef localRef
                ? localRef.underlying().actor().getClass()
                : event.recipient().getClass();
        String message = "unhandled message from " + event.sender() + ": " + event.message();
        publish(new Debug(event.recipient().path().toString(), actorClass, message));
    }

    @Override
    public void close() {
        eventStream.unsubscribe(this);
    }
}
