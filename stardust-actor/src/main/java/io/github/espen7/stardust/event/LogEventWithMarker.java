package io.github.espen7.stardust.event;

/** 带结构化 marker 的日志事件。 */
public interface LogEventWithMarker {
    /** @return marker，可以为 {@code null} */
    LogMarker marker();
}
