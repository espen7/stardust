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
 * Actor constructor 或初始 receive 创建失败时使用的异常。
 */
public class ActorInitializationException extends RuntimeException {
    /**
     * 创建带 cause 的初始化异常。
     *
     * @param cause 初始化失败原因
     */
    public ActorInitializationException(Throwable cause) {
        super(cause);
    }

    /**
     * 创建带消息的初始化异常。
     *
     * @param message 异常消息
     */
    public ActorInitializationException(String message) {
        super(message);
    }
}
