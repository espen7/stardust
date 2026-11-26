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

/**
 * Actor constructor 或初始 receive 创建失败时使用的异常。
 */
public class ActorInitializationException extends TapioException {
    private final ActorRef actor;

    /**
     * 创建带 cause 的初始化异常。
     *
     * @param cause 初始化失败原因
     */
    public ActorInitializationException(Throwable cause) {
        this(null, null, cause);
    }

    /**
     * 创建带消息的初始化异常。
     *
     * @param message 异常消息
     */
    public ActorInitializationException(String message) {
        this(null, message, null);
    }

    ActorInitializationException(String message, Throwable cause) {
        this(null, message, cause);
    }

    protected ActorInitializationException(ActorRef actor, String message, Throwable cause) {
        super(actor == null ? message : actor.path() + ": " + message, cause);
        this.actor = actor;
    }

    /** 内部创建入口，保留失败的 ActorRef。 */
    @InternalApi
    public static ActorInitializationException apply(ActorRef actor, String message, Throwable cause) {
        return new ActorInitializationException(actor, message, cause);
    }

    /**
     * 返回初始化失败的 Actor。
     *
     * @return 关联 ActorRef；未知时为 {@code null}
     */
    public ActorRef getActor() {
        return actor;
    }
}
