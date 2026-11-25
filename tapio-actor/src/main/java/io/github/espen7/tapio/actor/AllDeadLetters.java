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
 * EventStream 上 dead-letter 事件的共同订阅类型。
 */
public interface AllDeadLetters extends WrappedMessage {
    /**
     * 返回未投递消息的发送者。
     *
     * @return 消息原始发送者，或表示未指定发送者的 dead letters 引用
     */
    ActorRef sender();

    /**
     * 返回未投递消息的目标。
     *
     * @return 消息原始接收者
     */
    ActorRef recipient();
}
