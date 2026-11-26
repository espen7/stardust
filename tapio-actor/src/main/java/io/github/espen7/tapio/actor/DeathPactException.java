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

import io.github.espen7.tapio.TapioException;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.util.NoStackTrace;

/** 未处理 DeathWatch 终止通知时由 Actor 抛出的异常。 */
public final class DeathPactException extends TapioException implements NoStackTrace {
    private final ActorRef dead;

    /**
     * 创建未处理 Terminated 通知异常。
     *
     * @param dead 已终止的 ActorRef
     */
    @InternalApi
    public DeathPactException(ActorRef dead) {
        super("Monitored actor [" + dead + "] terminated");
        this.dead = dead;
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }

    /**
     * 返回触发异常的 ActorRef。
     *
     * @return 已终止的 ActorRef
     */
    public ActorRef dead() {
        return dead;
    }
}
