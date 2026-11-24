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

import io.github.espen7.stardust.dispatch.sysmsg.SystemMessage;
import io.github.espen7.stardust.event.EventStream;

/**
 * 将不可投递的消息发布到 EventStream 的 Actor 引用。
 *
 * <p>由 ActorSystem 的 Provider 创建。已包装的 {@link DeadLetter} 可以直接发布，
 * 普通消息先包装，再发布。带有 {@link DeadLetterSuppression} 标记的消息通过
 * {@link SuppressedDeadLetter} 发布。
 */
public final class DeadLetterActorRef extends EmptyLocalActorRef {
    DeadLetterActorRef(ActorRefProvider provider, ActorPath path, EventStream eventStream) {
        super(provider, path, eventStream);
    }

    /**
     * 处理无法投递的消息。按消息类型发布事件或处理 DeathWatch 系统消息。
     *
     * @param message 无法投递的消息或已包装的 dead-letter 事件
     * @param sender 原始发送者，可以为 {@link ActorRef#noSender}
     * @throws InvalidMessageException 消息为 {@code null}
     */
    @Override
    public void tell(Object message, ActorRef sender) {
        if (message == null) throw new InvalidMessageException("Message is null");
        if (message instanceof DeadLetter deadLetter) {
            if (!specialHandle(deadLetter.message(), deadLetter.sender())) eventStream.publish(deadLetter);
        } else if (!specialHandle(message, sender)) {
            eventStream.publish(new DeadLetter(message, sender == noSender ? provider().deadLetters() : sender, this));
        }
    }

    @Override
    protected boolean specialHandle(Object message, ActorRef sender) {
        if (message instanceof SystemMessage.Watch watch) {
            if (!watch.watchee.equals(this) && !watch.watcher.equals(this)) {
                ((InternalActorRef) watch.watcher).sendSystemMessage(
                        new SystemMessage.DeathWatchNotification(watch.watchee, false, false));
            }
            return true;
        }
        return super.specialHandle(message, sender);
    }
}
