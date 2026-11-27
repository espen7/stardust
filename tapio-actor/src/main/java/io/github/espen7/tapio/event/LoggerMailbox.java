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

import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.dispatch.Envelope;
import io.github.espen7.tapio.dispatch.MessageQueue;
import io.github.espen7.tapio.dispatch.UnboundedMailbox;

/** 无界 logger 队列；关闭时将满足级别的剩余日志同步刷到标准输出。 */
public final class LoggerMailbox extends UnboundedMailbox.MessageQueue implements LoggerMessageQueueSemantics {
    private final ActorSystem system;

    /** 创建属于指定 ActorSystem 的 logger 队列。 */
    public LoggerMailbox(ActorRef owner, ActorSystem system) {
        this.system = system;
    }

    @Override
    public void cleanUp(ActorRef owner, MessageQueue deadLetters) {
        if (hasMessages()) {
            LogEvent.Level logLevel = system.eventStream().logLevel();
            boolean loggingEnabled = logLevel != LogEvent.Level.OFF;
            Envelope envelope = dequeue();
            while (envelope != null) {
                if (loggingEnabled && envelope.message() instanceof LogEvent event
                        && event.level() != LogEvent.Level.OFF
                        && event.level().ordinal() >= logLevel.ordinal()) {
                    StandardOutLogger.INSTANCE.tell(envelope.message(), envelope.sender());
                }
                envelope = dequeue();
            }
        }
        super.cleanUp(owner, deadLetters);
    }
}
