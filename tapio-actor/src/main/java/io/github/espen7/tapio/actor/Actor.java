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

import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.japi.pf.PartialFunction;

import java.util.LinkedList;
import java.util.Optional;
import java.util.function.Function;

/** Classic Actor 的上下文、消息处理和生命周期。 */
public abstract class Actor {
    private static final Object NotHandled = new Object();
    private static final Function<Object, Object> notHandledFun = message -> NotHandled;

    protected final ActorContext context;
    protected final ActorRef self;

    /** 构造期间从 ActorCell 绑定当前 Actor 的上下文和引用。 */
    protected Actor() {
        LinkedList<ActorContext> contextStack = ActorCell.contextStack.get();
        if (contextStack.isEmpty() || contextStack.peekFirst() == null) {
            throw new ActorInitializationException(
                    "You cannot create an instance of [" + getClass().getName() + "] explicitly using the constructor (new). "
                            + "You have to use one of the 'actorOf' factory methods to create a new actor. See the documentation.");
        }
        context = contextStack.peekFirst();
        contextStack.addFirst(null);
        self = context.self();
    }

    /** @return 当前消息的 sender */
    public final ActorRef sender() {
        return context.sender();
    }

    /** @return Actor 的初始消息处理逻辑 */
    public abstract PartialFunction<Object, Void> receive();

    @InternalApi
    protected void aroundReceive(PartialFunction<Object, Void> receive, Object message) throws Exception {
        if (receive.applyOrElse(message, notHandledFun) == NotHandled) unhandled(message);
    }

    @InternalApi
    protected void aroundPreStart() throws Exception {
        preStart();
    }

    @InternalApi
    protected void aroundPostStop() throws Exception {
        postStop();
    }

    @InternalApi
    protected void aroundPreRestart(Throwable reason, Optional<Object> message) throws Exception {
        preRestart(reason, message);
    }

    @InternalApi
    protected void aroundPostRestart(Throwable reason) throws Exception {
        postRestart(reason);
    }

    /** @return 用于处理 child failure 的监督策略 */
    public SupervisorStrategy supervisorStrategy() {
        return SupervisorStrategy.defaultStrategy;
    }

    /** 在 Actor 开始处理消息前执行。 */
    public void preStart() throws Exception {
    }

    /** 在 Actor 终止时执行。 */
    public void postStop() throws Exception {
    }

    /** 默认解除对 children 的观察、停止 children，然后调用 postStop。 */
    public void preRestart(Throwable reason, Optional<Object> message) throws Exception {
        for (ActorRef child : context.children()) {
            context.unwatch(child);
            context.stop(child);
        }
        postStop();
    }

    /** 重建后默认调用 preStart。 */
    public void postRestart(Throwable reason) throws Exception {
        preStart();
    }

    /** 发布未处理消息。未处理的 Terminated 消息触发 DeathPactException。 */
    public void unhandled(Object message) {
        if (message instanceof Terminated terminated) throw new DeathPactException(terminated.actor());
        context.system().eventStream().publish(new UnhandledMessage(message, sender(), self));
    }
}
