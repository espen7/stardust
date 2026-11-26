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

import com.typesafe.config.Config;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSystem;

/**
 * 多生产者单消费者无界邮箱类型，对应 Pekko {@code SingleConsumerOnlyUnboundedMailbox}。
 */
public final class SingleConsumerOnlyUnboundedMailbox implements MailboxType, ProducesMessageQueue<NodeMessageQueue> {
    /** 创建单消费者无界邮箱类型实例。 */
    public SingleConsumerOnlyUnboundedMailbox() {
    }

    /**
     * 按反射邮箱契约创建单消费者无界邮箱类型实例。
     *
     * @param settings 系统设置
     * @param config 邮箱配置
     */
    public SingleConsumerOnlyUnboundedMailbox(ActorSystem.Settings settings, Config config) {
    }

    @Override
    public MessageQueue create(ActorRef owner, ActorSystem system) {
        return new NodeMessageQueue();
    }
}
