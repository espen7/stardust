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

import java.util.Optional;

/**
 * 不强制邮箱类型限制的暂存 Actor 基类，对应 Pekko {@code AbstractActorWithUnrestrictedStash}。
 *
 * <p>调用方须自行确保邮箱实现 {@link io.github.espen7.tapio.dispatch.DequeBasedMessageQueueSemantics}。</p>
 */
public abstract class AbstractActorWithUnrestrictedStash extends AbstractActor {
    private final StashSupport stash = new StashSupport(getContext(), getSelf());

    /** 创建支持消息暂存的 Actor 实例。 */
    protected AbstractActorWithUnrestrictedStash() {
    }

    /**
     * 将当前正在处理的消息暂存到私有队列中。
     *
     * @throws StashOverflowException 超出 {@code stash-capacity} 时
     * @throws IllegalStateException 重复暂存同一消息时
     */
    public void stash() {
        stash.stash();
    }

    /** 将最早暂存的一条消息重新放回邮箱头部。 */
    public void unstash() {
        stash.unstash();
    }

    /** 将所有暂存消息按原顺序放回邮箱头部并清空暂存队列。 */
    public void unstashAll() {
        stash.unstashAll();
    }

    @Override
    public void preRestart(Throwable reason, Optional<Object> message) throws Exception {
        try {
            unstashAll();
        } finally {
            super.preRestart(reason, message);
        }
    }

    @Override
    public void postStop() throws Exception {
        try {
            unstashAll();
        } finally {
            super.postStop();
        }
    }
}
