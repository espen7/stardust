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
package io.github.espen7.tapio.pattern;

import io.github.espen7.tapio.annotation.InternalApi;

import java.time.Duration;

/**
 * 退避计数重置策略，对应 Pekko {@code BackoffReset}。
 *
 * <p>INTERNAL API。</p>
 */
@InternalApi
public sealed interface BackoffReset permits BackoffReset.AutoReset, BackoffReset.ManualReset {
    /** 子 Actor 稳定运行超过指定时长后自动重置退避计数。 */
    record AutoReset(Duration resetBackoff) implements BackoffReset {
    }

    /** 仅在收到 {@link BackoffSupervisor#reset()} 时重置退避计数。 */
    final class ManualReset implements BackoffReset {
        public static final ManualReset INSTANCE = new ManualReset();

        private ManualReset() {
        }
    }
}
