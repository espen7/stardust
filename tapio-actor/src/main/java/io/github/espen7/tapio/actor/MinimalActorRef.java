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

import io.github.espen7.tapio.dispatch.sysmsg.SystemMessage;

/**
 * 没有 {@link ActorCell} 的本地内部引用。
 *
 * <p>默认忽略普通消息和系统消息。子类可按所代表的引用类型覆写投递行为。
 */
public abstract class MinimalActorRef extends InternalActorRef {
    @Override
    public void tell(Object message, ActorRef sender) {
    }

    @Override
    public void sendSystemMessage(SystemMessage message) {
    }
}
