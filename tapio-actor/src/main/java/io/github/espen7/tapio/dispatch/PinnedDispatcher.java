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
import io.github.espen7.tapio.actor.ActorSystem;

import java.time.Duration;

/**
 * 为单个 Actor 提供专用单线程的 Dispatcher。
 *
 * <p>每个 configurator 调用都会创建新实例；实例首次注册时绑定 owner，
 * owner 终止并注销后才允许绑定另一个 Actor。</p>
 */
public class PinnedDispatcher extends Dispatcher {
    private volatile ActorCell owner;

    PinnedDispatcher(ActorSystem system, ActorCell actor, String id, Duration shutdownTimeout,
                     ThreadPoolConfig threadPoolConfig) {
        super(Integer.MAX_VALUE, Duration.ZERO, system, system.scheduler(), shutdownTimeout,
                ThreadPoolExecutorConfigurator.createExecutorServiceFactory(
                        threadPoolConfig.withPoolSize(1, 1), id, system.threadFactory()));
        this.owner = actor;
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
