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

import io.github.espen7.tapio.event.LogMarker;

import java.util.Map;

/** Actor runtime 使用的公开日志 marker。 */
public final class ActorLogMarker {
    private ActorLogMarker() {
    }

    /**
     * 创建 dead-letter 日志 marker。
     *
     * @param messageClass dead letter 消息的 class 名称
     * @return 包含 {@code tapioMessageClass} 属性的 marker
     */
    public static LogMarker deadLetter(String messageClass) {
        return new LogMarker("tapioDeadLetter", Map.of(LogMarker.MESSAGE_CLASS, messageClass));
    }
}
