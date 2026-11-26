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

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

/** Actor 在配置的静默时间后收到的普通消息。 */
public record ReceiveTimeout(Duration timeout) implements PossiblyHarmful, Serializable {
    public ReceiveTimeout {
        Objects.requireNonNull(timeout, "timeout");
    }

    /**
     * 返回触发本次通知的静默时长。
     *
     * @return receive timeout 时长
     */
    public Duration getTimeout() {
        return timeout;
    }
}
