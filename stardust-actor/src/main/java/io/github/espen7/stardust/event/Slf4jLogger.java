package io.github.espen7.stardust.event;

import io.github.espen7.stardust.actor.AbstractActor;
import io.github.espen7.stardust.actor.UnhandledMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
}
