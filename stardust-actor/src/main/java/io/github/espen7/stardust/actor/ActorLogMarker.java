package io.github.espen7.stardust.actor;

import io.github.espen7.stardust.event.LogMarker;

import java.util.Map;

/** Actor runtime 使用的公开日志 marker。 */
public final class ActorLogMarker {
    private ActorLogMarker() {
    }

    /** 创建 dead-letter 日志 marker。 */
    public static LogMarker deadLetter(String messageClass) {
        return new LogMarker("pekkoDeadLetter", Map.of(LogMarker.MESSAGE_CLASS, messageClass));
    }
}
