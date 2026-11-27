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
import io.github.espen7.tapio.actor.ActorContext;
import io.github.espen7.tapio.actor.ActorPath;
import io.github.espen7.tapio.japi.pf.PartialFunction;

import java.util.Objects;

/**
 * 在 {@code tapio.actor.debug.receive = on} 时记录 Actor 收到消息（已处理/未处理）的 DEBUG 日志装饰器。
 */
public final class LoggingReceive implements PartialFunction<Object, Void> {
    private final PartialFunction<Object, Void> receive;
    private final ActorContext context;
    private final String label;
    private final LoggingAdapter log;

    /**
     * 使用默认日志标签创建 {@link LoggingReceive}。
     *
     * @param receive 被包装的 Receive
     * @param context 所属 ActorContext
     */
    public LoggingReceive(PartialFunction<Object, Void> receive, ActorContext context) {
        this(receive, context, null);
    }

    /**
     * 使用指定标签创建 {@link LoggingReceive}。
     *
     * @param receive 被包装的 Receive
     * @param context 所属 ActorContext
     * @param label 可选标签
     */
    public LoggingReceive(PartialFunction<Object, Void> receive, ActorContext context, String label) {
        this.receive = Objects.requireNonNull(receive, "receive must not be null");
        this.context = Objects.requireNonNull(context, "context must not be null");
        this.label = label;
        this.log = Logging.getLogger(context.system().eventStream(), context.self().path().toString(),
                LoggingReceive.class);
    }

    /**
     * 当启用 {@code tapio.actor.debug.receive} 时返回包装后的 {@link AbstractActor.Receive}，否则原样返回。
     *
     * @param receive 原始 Receive
     * @param context 所属 ActorContext
     * @return Receive 实例
     */
    public static AbstractActor.Receive create(AbstractActor.Receive receive, AbstractActor.ActorContext context) {
        if (context.system().settings().config().getBoolean(ActorPath.Scheme + ".actor.debug.receive")) {
            return new AbstractActor.Receive(new LoggingReceive(receive.onMessage(), context));
        }
        return receive;
    }

    @Override
    public boolean isDefinedAt(Object message) {
        boolean handled = receive.isDefinedAt(message);
        LogEvent.Level currentLevel = context.system().eventStream().logLevel();
        if (currentLevel != LogEvent.Level.OFF && LogEvent.Level.DEBUG.ordinal() >= currentLevel.ordinal()) {
            log.debug("received {} message {}{}",
                    handled ? "handled" : "unhandled",
                    message,
                    label == null ? "" : " in " + label);
        }
        return handled;
    }

    @Override
    public Void apply(Object message) throws Exception {
        return receive.apply(message);
    }
}
