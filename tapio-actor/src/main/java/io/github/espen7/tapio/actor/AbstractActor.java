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
package io.github.espen7.tapio.actor;

import io.github.espen7.tapio.dispatch.ExecutionContext;
import io.github.espen7.tapio.japi.pf.PartialFunction;
import io.github.espen7.tapio.japi.pf.ReceiveBuilder;

import java.time.Duration;
import java.util.Optional;

/** Classic Actor 的 lambda 友好 Java API。 */
public abstract class AbstractActor extends Actor {
    /** 封装可用于消息分派的偏函数。 */
    public static final class Receive {
        private final PartialFunction<Object, Void> onMessage;

        public Receive(PartialFunction<Object, Void> onMessage) {
            this.onMessage = onMessage;
        }

        public PartialFunction<Object, Void> onMessage() {
            return onMessage;
        }

        /** 当前 Receive 未匹配时使用给定的 Receive。 */
        public Receive orElse(Receive other) {
            return new Receive(onMessage.orElse(other.onMessage));
        }
    }

    /** 对所有消息均未定义的行为。 */
    public static final Receive emptyBehavior = new Receive(PartialFunction.empty());

    /** ActorContext 的 Java API，应在所属 Actor 的回调中使用。 */
    @io.github.espen7.tapio.annotation.DoNotInherit
    public interface ActorContext extends io.github.espen7.tapio.actor.ActorContext {
        default ActorRef getSelf() {
            return self();
        }

        default Props getProps() {
            return props();
        }

        default ActorRef getSender() {
            return sender();
        }

        default Iterable<ActorRef> getChildren() {
            return children();
        }

        default Optional<ActorRef> findChild(String name) {
            return child(name);
        }

        default ActorRef getParent() {
            return parent();
        }

        default ActorSystem getSystem() {
            return system();
        }

        default ExecutionContext getDispatcher() {
            return dispatcher();
        }

        /** 替换当前消息处理逻辑。 */
        default void become(Receive behavior) {
            become(behavior, true);
        }

        /** 替换或压入消息处理逻辑。 */
        default void become(Receive behavior, boolean discardOld) {
            become(behavior.onMessage(), discardOld);
        }

        default Duration getReceiveTimeout() {
            Duration timeout = receiveTimeout();
            return timeout == null ? Duration.ZERO : timeout;
        }

        default void cancelReceiveTimeout() {
            setReceiveTimeout(null);
        }
    }

    /** @return 所属 Actor 的 Java 上下文 */
    public ActorContext getContext() {
        return (ActorContext) context;
    }

    /** @return 此 Actor 的引用 */
    public ActorRef getSelf() {
        return self;
    }

    /** @return 当前消息的 sender */
    public ActorRef getSender() {
        return sender();
    }

    /** @return 一个新的消息匹配构建器 */
    public final ReceiveBuilder receiveBuilder() {
        return ReceiveBuilder.create();
    }

    /** @return Actor 的初始消息处理逻辑 */
    public abstract Receive createReceive();

    @Override
    public PartialFunction<Object, Void> receive() {
        return createReceive().onMessage();
    }
}
