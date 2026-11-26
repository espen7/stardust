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

import io.github.espen7.tapio.dispatch.MailboxType;
import io.github.espen7.tapio.dispatch.MessageDispatcher;
import io.github.espen7.tapio.dispatch.sysmsg.SystemMessage;

import java.util.Iterator;
import java.util.List;

/**
 * 本地 ActorRef，持有一个 ActorCell。
 *
 * <p>LocalActorRef 是公开引用和内部执行状态之间的桥梁。它不直接调用 Actor，
 * tell 和 stop 都转成 ActorCell 的消息或系统消息。</p>
 */
public class LocalActorRef extends ActorRefWithCell implements LocalRef {
    private final ActorSystemImpl system;
    private final ActorPath path;
    private final ActorCell actorCell;
    private final String name;

    /**
     * 构造完成后立即创建 ActorCell，并按指定 mailbox type 异步初始化。
     *
     * @param system 所属 ActorSystem
     * @param props Actor 创建配置
     * @param dispatcher 所属 Dispatcher
     * @param mailboxType 创建普通消息队列的 mailbox type
     * @param supervisor supervisor ActorRef
     * @param path ActorPath
     * @param name parent 下的 child name
     */
    public LocalActorRef(ActorSystemImpl system, Props props, MessageDispatcher dispatcher,
                         MailboxType mailboxType, InternalActorRef supervisor, ActorPath path, String name) {
        this(system, props, dispatcher, mailboxType, supervisor, path, name, true);
    }

    LocalActorRef(ActorSystemImpl system, Props props, MessageDispatcher dispatcher, MailboxType mailboxType,
                  InternalActorRef supervisor, ActorPath path, String name, boolean start) {
        this.system = system;
        this.path = path;
        this.name = name;
        this.actorCell = newActorCell(system, this, props, dispatcher, supervisor);
        actorCell.init(true, mailboxType);
        if (start) start();
    }

    @Override
    public void start() {
        actorCell.start();
    }

    @Override
    ActorSystemImpl system() {
        return system;
    }

    @Override
    InternalActorRef getParent() {
        return actorCell.parent();
    }

    @Override
    InternalActorRef getSingleChild(String name) {
        return actorCell.getSingleChild(name);
    }

    @Override
    List<ActorRef> children() {
        return actorCell.childrenRefs().children();
    }

    @Override
    Props props() {
        return actorCell.props();
    }

    @Override
    InternalActorRef getChild(Iterator<String> names) {
        InternalActorRef ref = this;
        while (names.hasNext() && ref instanceof LocalActorRef local) {
            String name = names.next();
            ref = switch (name) {
                case ".." -> local.getParent();
                case "" -> local;
                default -> local.getSingleChild(name);
            };
        }
        return ref == null ? Nobody.INSTANCE : (!names.hasNext() ? ref : ref.getChild(names));
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
    protected ActorCell newActorCell(ActorSystemImpl system, LocalActorRef ref, Props props, MessageDispatcher dispatcher, InternalActorRef supervisor) {
        return new ActorCell(system, ref, props, dispatcher, supervisor);
    }

    /**
     * 返回底层 ActorCell。
     *
     * @return 此引用对应的 ActorCell
     */
    @Override
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
    public void suspend() {
        actorCell.suspend();
    }

    @Override
    public void resume(Throwable causedByFailure) {
        actorCell.resume(causedByFailure);
    }

    @Override
    public void restart(Throwable cause) {
        actorCell.restart(cause);
    }

    @Override
    public void stop() {
        actorCell.stop();
    }

    @Override
    public boolean isTerminated() {
        return actorCell.isTerminated();
    }
}
