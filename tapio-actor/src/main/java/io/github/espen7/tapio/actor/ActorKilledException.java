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

/**
 * 表示 Actor 收到 Pekko Classic 的 Kill 指令后产生的异常。
 *
 * <p>默认监督策略对该异常执行 {@link SupervisorStrategy.Directive#Stop}。</p>
 */
public final class ActorKilledException extends RuntimeException {
    /**
     * 创建 ActorKilledException。
     *
     * @param message 异常消息
     */
    public ActorKilledException(String message) {
        super(message);
    }
}
