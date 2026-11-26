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
package io.github.espen7.tapio.event;

import com.typesafe.config.Config;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.dispatch.MailboxType;
import io.github.espen7.tapio.dispatch.ProducesMessageQueue;

/** 创建 logger mailbox 的配置类型。 */
public final class LoggerMailboxType implements MailboxType, ProducesMessageQueue<LoggerMailbox> {
    /** 供 mailbox 配置反射构造使用。 */
    public LoggerMailboxType(ActorSystem.Settings settings, Config config) {
    }

    @Override
    public io.github.espen7.tapio.dispatch.MessageQueue create(ActorRef owner, ActorSystem system) {
        if (owner == null || system == null) {
            throw new IllegalArgumentException("no mailbox owner or system given");
        }
        return new LoggerMailbox(owner, system);
    }
}
