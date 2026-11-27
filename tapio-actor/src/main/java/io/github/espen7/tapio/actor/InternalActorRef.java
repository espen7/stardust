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
import io.github.espen7.tapio.dispatch.sysmsg.SystemMessage;

import java.util.Iterator;

/**
 * 支持 Provider 查询和系统消息投递的 runtime 内部引用。
 */
@InternalApi
public abstract class InternalActorRef extends ActorRef implements ActorRefScope {
    /** 创建内部 ActorRef 抽象实例。 */
    protected InternalActorRef() {
    }

    /**
     * 返回引用的 Provider。
     *
     * @return 创建此引用的 {@link ActorRefProvider}
     */
    public abstract ActorRefProvider provider();

    @InternalApi
    public abstract InternalActorRef getParent();

    /**
     * 向此引用投递系统消息。
     *
     * @param message 尚未链接至任何 Mailbox 的系统消息实例
     */
    public abstract void sendSystemMessage(SystemMessage message);

    @InternalApi
    public InternalActorRef getChild(Iterator<String> names) {
        while (names.hasNext()) {
            if (!names.next().isEmpty()) return Nobody.INSTANCE;
        }
        return this;
    }

    public abstract void start();

    public abstract void resume(Throwable causedByFailure);

    public abstract void suspend();

    public abstract void restart(Throwable cause);

    public abstract void stop();

    @InternalApi
    public abstract boolean isTerminated();
}
