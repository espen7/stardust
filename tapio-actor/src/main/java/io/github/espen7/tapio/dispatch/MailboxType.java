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

import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSystem;

/**
 * 为 ActorCell 创建普通消息队列的工厂，对应上游 {@code dispatch.MailboxType}。
 */
public interface MailboxType {

    /**
     * 创建一个新的普通消息队列。
     *
     * @param owner 拥有此 Mailbox 的 ActorRef
     * @param system 所属 ActorSystem
     * @return 新的普通消息队列
     */
    MessageQueue create(ActorRef owner, ActorSystem system);
}
