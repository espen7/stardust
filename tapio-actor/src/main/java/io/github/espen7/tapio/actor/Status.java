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

import io.github.espen7.tapio.annotation.DoNotInherit;

import java.io.Serializable;

/**
 * Classic Actor 用于回复操作结果的消息。
 *
 * @see io.github.espen7.tapio.pattern.Patterns#ask
 */
@DoNotInherit
public sealed interface Status extends Serializable permits Status.Success, Status.Failure {
    /**
     * 表示操作成功。ask 解包此消息并以结果值完成。
     *
     * @param status 操作结果，可以为 {@code null}
     */
    record Success(Object status) implements Status {}

    /**
     * 表示操作失败。ask 以提供的原因失败。
     *
     * @param cause 操作失败的原因，须为非 {@code null}
     */
    record Failure(Throwable cause) implements Status {}
}
