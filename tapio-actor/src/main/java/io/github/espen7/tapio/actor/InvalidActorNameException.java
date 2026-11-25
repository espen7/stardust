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

/** Actor 名称无效或已被同一父 Actor 的另一个子引用使用。 */
public final class InvalidActorNameException extends IllegalArgumentException {
    /**
     * 创建名称异常。
     *
     * @param message 异常消息
     */
    public InvalidActorNameException(String message) {
        super(message);
    }
}
