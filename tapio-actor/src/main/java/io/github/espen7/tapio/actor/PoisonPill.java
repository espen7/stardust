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

/** ActorCell 自动处理并永久停止当前 Actor 的消息。 */
public abstract class PoisonPill implements AutoReceivedMessage, PossiblyHarmful, DeadLetterSuppression, Serializable {
    private static final PoisonPill INSTANCE = new PoisonPill() {
    };

    private PoisonPill() {
    }

    /**
     * 返回 PoisonPill 单例。
     *
     * @return 共享的 PoisonPill 实例
     */
    public static PoisonPill getInstance() {
        return INSTANCE;
    }

    protected final Object readResolve() {
        return INSTANCE;
    }

    @Override
    public String toString() {
        return "PoisonPill";
    }
}
