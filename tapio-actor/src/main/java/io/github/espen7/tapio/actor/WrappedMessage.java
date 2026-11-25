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
 * 包含原始消息的信封。
 */
public interface WrappedMessage {
    /**
     * 递归取得被信封包装的原始消息。
     *
     * @param message 待展开的消息
     * @return 最内层消息
     */
    static Object unwrap(Object message) {
        while (message instanceof WrappedMessage wrapped) message = wrapped.message();
        return message;
    }
    /**
     * 返回信封中的消息。
     *
     * @return 包装前的原始消息
     */
    Object message();
}
