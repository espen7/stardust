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
package io.github.espen7.tapio.dispatch;

import java.time.Duration;

/**
 * 标记会创建带 push-timeout 语义 mailbox 的 mailbox type，对应上游
 * {@code dispatch/Mailbox.scala} 的 {@code ProducesPushTimeoutSemanticsMailbox}。
 *
 * <p>runtime 用它在创建时校验非零 push-timeout 并发布告警。</p>
 */
interface ProducesPushTimeoutSemanticsMailbox {

    /**
     * @return 入队超时；负值表示无限等待
     */
    Duration pushTimeOut();
}
