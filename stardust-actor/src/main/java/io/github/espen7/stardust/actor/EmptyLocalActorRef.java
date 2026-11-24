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
 * 代表本地查找未命中的内部引用。
 *
 * <p>始终处于终止状态，处理指向此引用的 DeathWatch 消息，并将无法投递的普通消息发布到 EventStream。
 */
class EmptyLocalActorRef extends MinimalActorRef {
    private final ActorRefProvider provider;
    private final ActorPath path;
    /** 发布 dead letters 和 DeathWatch 通知所用的 EventStream。 */
    protected final EventStream eventStream;

    EmptyLocalActorRef(ActorRefProvider provider, ActorPath path, EventStream eventStream) {
        this.provider = provider;
        this.path = path;
        this.eventStream = eventStream;
    }

    @Override
    public ActorRefProvider provider() {
        return provider;
    }

    @Override
    public ActorPath path() {
        return path;
    }

    @Override
    public boolean isTerminated() {
        return true;
    }

    @Override
    public void sendSystemMessage(SystemMessage message) {
        specialHandle(message, provider.deadLetters());
    }

    @Override
    public void tell(Object message, ActorRef sender) {
        if (message == null) throw new InvalidMessageException("Message is null");
        if (message instanceof DeadLetter deadLetter) {
            specialHandle(deadLetter.message(), deadLetter.sender());
        } else if (!specialHandle(message, sender)) {
            eventStream.publish(new DeadLetter(message, sender == noSender ? provider.deadLetters() : sender, this));
        }
    }

    /**
     * 处理 DeathWatch 消息及标记为 {@link DeadLetterSuppression} 的消息。
     *
     * @param message 待处理的消息
     * @param sender 原始发送者，可以为 {@link ActorRef#noSender}
     * @return 已处理消息时为 {@code true}
     */
    protected boolean specialHandle(Object message, ActorRef sender) {
        if (message instanceof SystemMessage.Watch watch) {
            if (watch.watchee.equals(this) && !watch.watcher.equals(this)) {
                ((InternalActorRef) watch.watcher).sendSystemMessage(
                        new SystemMessage.DeathWatchNotification(watch.watchee, false, false));
            }
            return true;
        }
        if (message instanceof SystemMessage.Unwatch) return true;
        if (message instanceof Identify identify) {
            (sender == noSender ? provider.deadLetters() : sender)
                    .tell(new ActorIdentity(identify.messageId(), java.util.Optional.empty()), this);
            return true;
        }
        if (message instanceof DeadLetterSuppression suppressed) {
            eventStream.publish(new SuppressedDeadLetter(suppressed, sender == noSender ? provider.deadLetters() : sender, this));
            return true;
        }
        return false;
    }
}
