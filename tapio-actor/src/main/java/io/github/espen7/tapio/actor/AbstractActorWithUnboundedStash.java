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

import io.github.espen7.tapio.dispatch.RequiresMessageQueue;
import io.github.espen7.tapio.dispatch.UnboundedDequeBasedMessageQueueSemantics;

/**
 * 强制使用无界双端邮箱的暂存 Actor 基类。
 */
public abstract class AbstractActorWithUnboundedStash extends AbstractActorWithUnrestrictedStash
        implements RequiresMessageQueue<UnboundedDequeBasedMessageQueueSemantics> {
    /** 创建支持无界消息暂存的 Actor 实例。 */
    protected AbstractActorWithUnboundedStash() {
    }
}
