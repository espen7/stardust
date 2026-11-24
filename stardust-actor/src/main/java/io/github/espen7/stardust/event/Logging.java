package io.github.espen7.stardust.event;

import java.util.Objects;
import io.github.espen7.stardust.actor.ActorRef;
import io.github.espen7.stardust.actor.ActorSystemImpl;
import io.github.espen7.stardust.actor.Props;

/** 创建绑定 EventStream 和 source 元数据的 LoggingAdapter。 */
public final class Logging {
    private Logging() {
    }

    public static LoggingAdapter getLogger(EventStream eventStream, String source, Class<?> sourceClass) {
        return new LoggingAdapter(eventStream, source, sourceClass);
    }

    public static LoggingAdapter getLogger(EventStream eventStream, Object source, Class<?> sourceClass) {
        return getLogger(eventStream, Objects.toString(source), sourceClass);
    }

    /** 创建系统 logger Actor，并订阅日志事件。 */
    public static ActorRef installSlf4jLogger(ActorSystemImpl system, boolean debugUnhandled) {
        EventStream eventStream = system.eventStream();
        ActorRef logger = system.actorOf(
                Props.create(Slf4jLogger.class, eventStream.logLevel(), debugUnhandled), "eventStreamLogger");
        eventStream.subscribe(logger, LogEvent.class);
        if (debugUnhandled) {
            eventStream.subscribe(logger, io.github.espen7.stardust.actor.UnhandledMessage.class);
        }
        return logger;
    }
}
