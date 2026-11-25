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

import java.time.Duration;
import java.util.concurrent.Executor;

/**
 * {@link ActorSystem} 的一次性调度入口。
 */
public abstract class Scheduler implements AutoCloseable {
    /**
     * 调度一次任务，在触发任务的线程运行。
     *
     * @param delay 任务延迟。非正时在调用线程直接运行
     * @param runnable 到期任务
     * @return 用于取消尚未执行的任务
     * @throws IllegalStateException 调度器已关闭
     * @throws IllegalArgumentException 延迟大于调度器支持的最大值
     */
    public final Cancellable scheduleOnce(Duration delay, Runnable runnable) {
        return scheduleOnce(delay, runnable, Runnable::run);
    }

    /**
     * 在指定延迟后将任务交给 executor 执行。非正延迟直接提交给 executor。
     *
     * @param delay 任务延迟
     * @param runnable 到期任务
     * @param executor 执行任务的 Executor
     * @return 用于取消任务的句柄
     * @throws IllegalStateException 调度器已关闭
     * @throws IllegalArgumentException 延迟大于调度器支持的最大值
     */
    public abstract Cancellable scheduleOnce(Duration delay, Runnable runnable, Executor executor);

    /**
     * 在指定延迟后向 Actor 投递一条消息。
     *
     * @param delay 投递延迟。非正时在调用线程发起投递
     * @param receiver 目标 Actor 引用
     * @param message 要投递的消息
     * @param sender 消息发送者，可以为 {@link ActorRef#noSender}
     * @return 用于取消尚未执行的投递任务
     * @throws IllegalStateException 调度器已关闭
     * @throws IllegalArgumentException 延迟大于调度器支持的最大值
     */
    public final Cancellable scheduleOnce(Duration delay, ActorRef receiver, Object message, ActorRef sender) {
        return scheduleOnce(delay, () -> receiver.tell(message, sender));
    }

    /**
     * 关闭调度器并停止接收新任务。
     *
     * <p>仍待执行的 {@link TaskRunOnClose} 任务会在关闭时运行。
     */
    @Override
    public abstract void close();

    /**
     * 关闭调度器时仍需执行的 runtime 内部任务。
     */
    public interface TaskRunOnClose extends Runnable {
    }
}
