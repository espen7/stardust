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
package io.github.espen7.tapio.dispatch;

import io.github.espen7.tapio.actor.ActorCell;

import java.time.Duration;

/**
 * 为单个 Actor 提供专用单线程的 Dispatcher。
 *
 * <p>每个 configurator 调用都会创建新实例；实例首次注册时绑定 owner，
 * owner 终止并注销后才允许绑定另一个 Actor。</p>
 */
public class PinnedDispatcher extends Dispatcher {
    private volatile ActorCell owner;

    /**
     * 创建绑定初始 owner 的 PinnedDispatcher。
     *
     * @param _configurator 创建此 Dispatcher 的 configurator
     * @param _actor 初始 owner，尚未绑定时为 {@code null}
     * @param _id 完整配置 id
     * @param _shutdownTimeout 没有 Actor 注册后等待 shutdown 的时间
     * @param _threadPoolConfig 单线程 executor 的基础配置
     */
    public PinnedDispatcher(MessageDispatcherConfigurator _configurator, ActorCell _actor, String _id,
                     Duration _shutdownTimeout, ThreadPoolConfig _threadPoolConfig) {
        super(_configurator, _id, Integer.MAX_VALUE, Duration.ZERO,
                (id, threadFactory) -> ThreadPoolExecutorConfigurator.createExecutorServiceFactory(
                        _threadPoolConfig.withPoolSize(1, 1), id, threadFactory),
                _shutdownTimeout);
        this.owner = _actor;
    }

    @Override
    public void register(ActorCell actorCell) {
        ActorCell actor = owner;
        if (actor != null && actorCell != actor) {
            throw new IllegalArgumentException("Cannot register to anyone but " + actor);
        }
        owner = actorCell;
        super.register(actorCell);
    }

    @Override
    public void unregister(ActorCell actor) {
        super.unregister(actor);
        owner = null;
    }
}
