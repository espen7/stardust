package io.github.espen7.stardust.event;

import java.util.Objects;
import java.util.function.Consumer;

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

    /** 安装 SLF4J bridge，并返回用于关闭订阅的句柄。 */
    public static AutoCloseable installSlf4jLogger(EventStream eventStream, boolean debugUnhandled) {
        Consumer<Object> bridge = new Slf4jLogger(eventStream, debugUnhandled);
        eventStream.subscribe(bridge);
        return () -> eventStream.unsubscribe(bridge);
    }
}
