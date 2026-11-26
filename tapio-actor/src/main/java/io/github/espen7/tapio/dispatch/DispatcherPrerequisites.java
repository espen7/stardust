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

import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.Scheduler;
import io.github.espen7.tapio.event.EventStream;

import java.util.Optional;
import java.util.concurrent.ThreadFactory;

/** 构造 MessageDispatcher 时使用的 ActorSystem 运行时组件。 */
public interface DispatcherPrerequisites {
    /**
     * 返回系统线程工厂。
     *
     * @return 线程工厂
     */
    ThreadFactory threadFactory();

    /**
     * 返回系统事件总线。
     *
     * @return 事件总线
     */
    EventStream eventStream();

    /**
     * 返回系统 Scheduler。
     *
     * @return Scheduler
     */
    Scheduler scheduler();

    /**
     * 返回系统设置。
     *
     * @return 系统设置
     */
    ActorSystem.Settings settings();

    /**
     * 返回系统 mailbox 注册表。
     *
     * @return mailbox 注册表
     */
    Mailboxes mailboxes();

    /**
     * 返回可选的外部默认 ExecutionContext。
     *
     * @return 存在时返回外部执行上下文，否则返回空
     */
    Optional<ExecutionContext> defaultExecutionContext();
}
