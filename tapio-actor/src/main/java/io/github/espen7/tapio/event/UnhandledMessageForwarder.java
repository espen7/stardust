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

import io.github.espen7.tapio.actor.AbstractActor;
import io.github.espen7.tapio.actor.UnhandledMessage;

/** 将未处理消息转换为 DEBUG 日志事件。 */
final class UnhandledMessageForwarder extends AbstractActor {
    @Override
    public Receive createReceive() {
        return receiveBuilder().match(UnhandledMessage.class, event ->
                getContext().system().eventStream().publish(new Debug(event.recipient().path().toString(),
                        event.recipient().getClass(),
                        "unhandled message from " + event.sender() + ": " + event.message()))).build();
    }
}
