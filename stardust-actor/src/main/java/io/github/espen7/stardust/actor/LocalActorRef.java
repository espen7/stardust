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
package io.github.espen7.stardust.actor;

import io.github.espen7.stardust.dispatch.Dispatcher;
import io.github.espen7.stardust.dispatch.sysmsg.SystemMessage;

/**
 * 本地 ActorRef，持有一个 ActorCell。
 *
 * <p>LocalActorRef 是公开引用和内部执行状态之间的桥梁。它不直接调用 Actor，
 * tell 和 stop 都转成 ActorCell 的消息或系统消息。</p>
 */
public class LocalActorRef extends InternalActorRef {
    private final ActorSystemImpl system;
    private final ActorPath path;
    private final ActorCell actorCell;
    private final String name;

    /**
     * 构造完成后立即创建 ActorCell，并触发异步初始化。
     *
     * @param system 所属 ActorSystem
     * @param props Actor 创建配置
     * @param dispatcher 所属 Dispatcher
     * @param supervisor supervisor ActorRef
     * @param path ActorPath
     * @param name parent 下的 child name
     */
    public LocalActorRef(ActorSystemImpl system, Props props, Dispatcher dispatcher, ActorRef supervisor, ActorPath path, String name) {
        this.system = system;
        this.path = path;
        this.name = name;
        this.actorCell = newActorCell(system, this, props, dispatcher, supervisor);
        actorCell.init(true);
    }

    /**
     * 可覆写的 ActorCell factory，对应 Pekko LocalActorRef.newActorCell。
     *
     * @param system 所属 ActorSystem
     * @param ref 待绑定的 LocalActorRef
     * @param props Actor 创建配置
     * @param dispatcher 所属 Dispatcher
     * @param supervisor supervisor ActorRef
     * @return 新 ActorCell
     */
    protected ActorCell newActorCell(ActorSystemImpl system, LocalActorRef ref, Props props, Dispatcher dispatcher, ActorRef supervisor) {
        return new ActorCell(system, ref, props, dispatcher, supervisor);
    }

    /**
     * 返回底层 ActorCell。
     *
     * @return 此引用对应的 ActorCell
     */
    public ActorCell underlying() {
        return actorCell;
    }

    /**
     * 返回 child name。
     *
     * @return child name
     */
    public String name() {
        return name;
    }

    @Override
    public ActorPath path() {
        return path;
    }

    /**
     * 将用户消息交给 ActorCell，由 Dispatcher 和 Mailbox 异步处理。
     *
     * @param message 待投递的消息
     * @param sender 消息发送者，可以为 {@link ActorRef#noSender}
     * @throws InvalidMessageException 消息为 {@code null}
     */
    @Override
    public void tell(Object message, ActorRef sender) {
        actorCell.sendMessage(message, sender);
    }

    @Override
    public ActorRefProvider provider() {
        return system.provider();
    }

    @Override
    public void sendSystemMessage(SystemMessage message) {
        actorCell.sendSystemMessage(message);
    }

    @Override
    public void stop() {
        actorCell.sendSystemMessage(new SystemMessage.Terminate());
    }

    @Override
    public boolean isTerminated() {
        return actorCell.isTerminated();
    }
}
