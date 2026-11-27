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

import java.time.Duration;

/**
 * 为 Actor 提供绑定其生命周期的定时自发消息能力。
 *
 * <p>通过 {@link AbstractActorWithTimers} 或 {@link AbstractLoggingActorWithTimers} 获取。
 * 所有定时器会在 Actor 重启或停止时自动取消；{@code TimerScheduler} 非线程安全，只能在拥有它的
 * Actor 内部使用。</p>
 */
@DoNotInherit
public abstract class TimerScheduler {
    /** 创建 TimerScheduler 实例。 */
    protected TimerScheduler() {
    }

    /**
     * 以固定间隔（fixed-delay）周期性向 {@code self} 发送消息。
     *
     * <p>若同一 {@code key} 已有定时器，会先取消旧定时器并保证其已入队但未处理的过期消息被丢弃。</p>
     *
     * @param key 定时器键
     * @param msg 待发送消息
     * @param delay 发送间隔
     */
    public abstract void startTimerWithFixedDelay(Object key, Object msg, Duration delay);

    /**
     * 在初始延迟后，以固定间隔（fixed-delay）周期性向 {@code self} 发送消息。
     *
     * @param key 定时器键
     * @param msg 待发送消息
     * @param initialDelay 初始延迟
     * @param delay 后续发送间隔
     */
    public abstract void startTimerWithFixedDelay(Object key, Object msg, Duration initialDelay, Duration delay);

    /**
     * 以固定频率（fixed-rate）周期性向 {@code self} 发送消息。
     *
     * @param key 定时器键
     * @param msg 待发送消息
     * @param interval 发送周期
     */
    public abstract void startTimerAtFixedRate(Object key, Object msg, Duration interval);

    /**
     * 在初始延迟后，以固定频率（fixed-rate）周期性向 {@code self} 发送消息。
     *
     * @param key 定时器键
     * @param msg 待发送消息
     * @param initialDelay 初始延迟
     * @param interval 发送周期
     */
    public abstract void startTimerAtFixedRate(Object key, Object msg, Duration initialDelay, Duration interval);

    /**
     * 启动一次性定时器，在指定超时后向 {@code self} 发送消息。
     *
     * @param key 定时器键
     * @param msg 待发送消息
     * @param timeout 超时时间
     */
    public abstract void startSingleTimer(Object key, Object msg, Duration timeout);

    /**
     * 判断指定 {@code key} 的定时器当前是否处于激活状态。
     *
     * @param key 定时器键
     * @return 激活返回 {@code true}
     */
    public abstract boolean isTimerActive(Object key);

    /**
     * 取消指定 {@code key} 的定时器。
     *
     * @param key 定时器键
     */
    public abstract void cancel(Object key);

    /** 取消当前所有已注册的定时器。 */
    public abstract void cancelAll();
}
